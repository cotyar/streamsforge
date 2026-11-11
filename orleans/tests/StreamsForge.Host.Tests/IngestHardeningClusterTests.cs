using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Ingest;
using StreamsForge.Engine;
using StreamsForge.Host.Auth;
using StreamsForge.Host.Facades;
using StreamsForge.Host.Services;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring <c>IngestTestSiloConfigurator</c> (IngestFacadeClusterTests.cs) —
/// duplicated per that file's own "xunit test classes shouldn't share cluster state" rationale.</summary>
internal sealed class IngestHardeningSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class IngestHardeningClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 009 A1 cluster tests, exercising the REAL <c>AddOrleansFacades()</c> DI wiring (not a hand-built
/// facade) against a real TestingHost cluster: A1.1 batch idempotency + row-level dedup, A1.2 per-source
/// push keys, and A1.3 cluster-wide aggregated counters via <see cref="IIngressStatsGrain"/>.
/// </summary>
public sealed class IngestHardeningClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<IngestHardeningSiloConfigurator>();
        builder.AddClientBuilderConfigurator<IngestHardeningClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static List<FieldDef> Fields => [new FieldDef("price", FieldType.Double), new FieldDef("id", FieldType.String)];

    private static string FreshName(string prefix) => $"{prefix}_{Guid.NewGuid():n}"[..16];

    /// <summary>One simulated replica: its OWN DI container (own SourceIngressRegistry, idempotency
    /// cache, key-usage tracker, stats-report-tracker baseline) wired to the SAME cluster client — the
    /// same "separate host process, shared cluster" shape a real multi-replica deployment has.</summary>
    private sealed class Replica
    {
        public IIngressFacade Ingress { get; }
        public SourceIngressRegistry Registry { get; }
        public IngressStatsReportTracker StatsTracker { get; }

        public Replica(TestCluster cluster)
        {
            var services = new ServiceCollection();
            services.AddSingleton<IClusterClient>(cluster.Client);
            services.AddOrleansFacades();
            var provider = services.BuildServiceProvider();
            Ingress = provider.GetRequiredService<IIngressFacade>();
            Registry = provider.GetRequiredService<SourceIngressRegistry>();
            StatsTracker = provider.GetRequiredService<IngressStatsReportTracker>();
        }

        public Task ReportStatsAsync(IReadOnlyList<SourceDefinition> sources, TestCluster cluster) =>
            IngestDrainPumpService.ReportStatsAsync(sources, Registry, cluster.Client, StatsTracker, NullLogger.Instance, CancellationToken.None);
    }

    private async Task SeedSourceAsync(string name, IngestConfig? ingest = null)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = SourceKinds.Ingest,
            Enabled = true,
            Fields = Fields,
            Ingest = ingest ?? new IngestConfig { CapacityRows = 1000, MaxBatchRows = 1000 },
        });
    }

    // ------------------------------------------------------------------
    // A1.1 — batch-level idempotency.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Repeated_idempotency_key_admits_once_and_replays_the_first_result()
    {
        var name = FreshName("idem");
        await SeedSourceAsync(name);
        var replica = new Replica(_cluster);

        var first = await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false, "retry-key-1");
        var second = await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false, "retry-key-1");

        Assert.False(first.Replayed);
        Assert.True(second.Replayed);
        Assert.Equal(first.Accepted, second.Accepted);
        Assert.Equal(2, second.Accepted);

        // The replay admitted NOTHING new — depth/TotalAccepted reflect only the first push.
        var status = await replica.Ingress.GetStatusAsync(name);
        Assert.Equal(2, status!.DepthRows);
        Assert.Equal(2, status.TotalAccepted);
    }

    [Fact]
    public async Task No_idempotency_key_admits_every_push_independently()
    {
        var name = FreshName("noidem");
        await SeedSourceAsync(name);
        var replica = new Replica(_cluster);

        await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }], partial: false);
        await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }], partial: false);

        var status = await replica.Ingress.GetStatusAsync(name);
        Assert.Equal(2, status!.TotalAccepted); // both admitted — no key, no replay
    }

    // ------------------------------------------------------------------
    // A1.1 — row-level dedup.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Row_level_duplicate_is_counted_as_Duplicate_not_Dropped()
    {
        var name = FreshName("dedup");
        await SeedSourceAsync(name, new IngestConfig { CapacityRows = 1000, MaxBatchRows = 1000, DedupKeyField = "id" });
        var replica = new Replica(_cluster);

        var result = await replica.Ingress.PushAsync(name,
            [new() { ["id"] = "r1", ["price"] = 1.0 }, new() { ["id"] = "r1", ["price"] = 2.0 }, new() { ["id"] = "r2", ["price"] = 3.0 }],
            partial: false);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted); // r1 (first) + r2
        Assert.Equal(1, result.Duplicate); // second r1
        Assert.Equal(0, result.Dropped);

        var status = await replica.Ingress.GetStatusAsync(name);
        Assert.Equal(1, status!.TotalDuplicate);
    }

    // ------------------------------------------------------------------
    // A1.2 — per-source push keys.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ValidateKeyAsync_a_source_with_zero_keys_is_JWT_only_not_open()
    {
        var name = FreshName("nokeys");
        await SeedSourceAsync(name, new IngestConfig()); // Keys is empty by default
        var replica = new Replica(_cluster);

        Assert.False(await replica.Ingress.ValidateKeyAsync(name, "anything-at-all"));
        Assert.False(await replica.Ingress.ValidateKeyAsync(name, null));
        Assert.False(await replica.Ingress.ValidateKeyAsync(name, ""));
    }

    [Fact]
    public async Task ValidateKeyAsync_accepts_the_matching_secret_and_rejects_a_wrong_one()
    {
        var name = FreshName("haskey");
        const string secret = "sfk_test-secret-value";
        var (hash, salt) = PasswordHasher.Hash(secret);
        await SeedSourceAsync(name, new IngestConfig
        {
            CapacityRows = 10,
            MaxBatchRows = 10,
            Keys = [new IngestKey { Id = "k1", Hash = hash, Salt = salt, Label = "test" }],
        });
        var replica = new Replica(_cluster);

        Assert.True(await replica.Ingress.ValidateKeyAsync(name, secret));
        Assert.False(await replica.Ingress.ValidateKeyAsync(name, "wrong-secret"));
    }

    [Fact]
    public async Task ValidateKeyAsync_returns_false_for_an_unknown_or_non_ingest_source()
    {
        var replica = new Replica(_cluster);
        Assert.False(await replica.Ingress.ValidateKeyAsync(FreshName("nope"), "key"));

        var genName = FreshName("gen");
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition { Name = genName, Kind = SourceKinds.Generator, Enabled = true, Fields = Fields, EventsPerSecond = 1 });
        Assert.False(await replica.Ingress.ValidateKeyAsync(genName, "key"));
    }

    [Fact]
    public async Task A_revoked_key_no_longer_validates()
    {
        var name = FreshName("revoke");
        const string secret = "sfk_revoke-me";
        var (hash, salt) = PasswordHasher.Hash(secret);
        await SeedSourceAsync(name, new IngestConfig
        {
            CapacityRows = 10,
            MaxBatchRows = 10,
            Keys = [new IngestKey { Id = "k1", Hash = hash, Salt = salt, Label = "test" }],
        });
        var replica = new Replica(_cluster);
        Assert.True(await replica.Ingress.ValidateKeyAsync(name, secret));

        // Revoke: remove the key and re-store, exactly what DELETE /{name}/ingest/keys/{id} does.
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var src = (await registry.GetSourceAsync(name))!;
        src.Ingest!.Keys.RemoveAll(k => k.Id == "k1");
        await registry.UpsertSourceAsync(src);

        Assert.False(await replica.Ingress.ValidateKeyAsync(name, secret));
    }

    // ------------------------------------------------------------------
    // A1.3 — cluster-wide aggregated counters.
    // ------------------------------------------------------------------

    [Fact]
    public async Task GetStatusAsync_shows_this_replicas_own_push_live_even_with_no_stats_report_yet()
    {
        var name = FreshName("live");
        await SeedSourceAsync(name);
        var replica = new Replica(_cluster);

        await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false);

        var status = await replica.Ingress.GetStatusAsync(name);
        Assert.True(status!.Aggregated);
        Assert.Equal(2, status.TotalAccepted); // live pending delta, no ReportStatsAsync tick needed
        Assert.False(string.IsNullOrEmpty(status.InstanceId));
    }

    [Fact]
    public async Task Two_replicas_reporting_into_the_same_stats_grain_sum_to_a_true_cluster_total()
    {
        var name = FreshName("agg");
        await SeedSourceAsync(name);
        var replicaA = new Replica(_cluster);
        var replicaB = new Replica(_cluster);

        await replicaA.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false); // 2 rows on A
        await replicaB.Ingress.PushAsync(name, [new() { ["price"] = 3.0 }, new() { ["price"] = 4.0 }, new() { ["price"] = 5.0 }], partial: false); // 3 rows on B

        var sources = await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetSourcesAsync();
        await replicaA.ReportStatsAsync(sources, _cluster);
        await replicaB.ReportStatsAsync(sources, _cluster);

        var statusFromA = await replicaA.Ingress.GetStatusAsync(name);
        var statusFromB = await replicaB.Ingress.GetStatusAsync(name);

        Assert.Equal(5, statusFromA!.TotalAccepted); // 2 (A) + 3 (B), visible from EITHER replica
        Assert.Equal(5, statusFromB!.TotalAccepted);
        Assert.True(statusFromA.Aggregated);
        Assert.True(statusFromB.Aggregated);
    }

    [Fact]
    public async Task ReportStatsAsync_twice_in_a_row_without_new_pushes_does_not_double_count()
    {
        var name = FreshName("noreDouble");
        await SeedSourceAsync(name);
        var replica = new Replica(_cluster);
        await replica.Ingress.PushAsync(name, [new() { ["price"] = 1.0 }], partial: false);

        var sources = await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetSourcesAsync();
        await replica.ReportStatsAsync(sources, _cluster);
        await replica.ReportStatsAsync(sources, _cluster); // nothing new happened — must be a no-op

        var status = await replica.Ingress.GetStatusAsync(name);
        Assert.Equal(1, status!.TotalAccepted);
    }
}
