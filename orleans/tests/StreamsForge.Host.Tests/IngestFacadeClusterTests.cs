using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Facades;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring ConnectorTestSiloConfigurator/PushStreamTestSiloConfigurator
/// (memory streams under the pull transport, memory grain storage) — duplicated per that file's own
/// "xunit test classes shouldn't share cluster state" rationale.</summary>
internal sealed class IngestTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class IngestTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 008 W4 cluster tests: <see cref="OrleansIngressFacade"/> (the real <see cref="IIngressFacade"/>
/// resolved through the real <c>AddOrleansFacades()</c> DI wiring — not a hand-built instance, so this
/// also proves the DI registration itself is correct) against a real TestingHost cluster with a real
/// RegistryGrain and real memory streams. Also covers the plan 006/008 "latent defect" fix: pushing
/// into a non-ingest source is 409 (WrongKind), and <see cref="IConnectorStatusFacade"/> no longer
/// activates a pointless IConnectorGrain for an ingest-kind source.
/// </summary>
public sealed class IngestFacadeClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private IIngressFacade _ingress = null!;
    private IConnectorStatusFacade _connectorStatus = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<IngestTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<IngestTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // Real production wiring (OrleansFacadesExtensions.AddOrleansFacades), not a hand-built facade
        // — the only thing swapped in is the TestCluster's IClusterClient.
        var services = new ServiceCollection();
        services.AddSingleton<IClusterClient>(_cluster.Client);
        services.AddOrleansFacades();
        var provider = services.BuildServiceProvider();
        _ingress = provider.GetRequiredService<IIngressFacade>();
        _connectorStatus = provider.GetRequiredService<IConnectorStatusFacade>();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static List<FieldDef> Fields => [new FieldDef("price", FieldType.Double)];

    private async Task SeedSourceAsync(string name, string kind, IngestConfig? ingest = null)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = kind,
            Enabled = true,
            Fields = Fields,
            Ingest = ingest,
            EventsPerSecond = 5,
        });
    }

    private static string FreshName(string prefix) => $"{prefix}_{Guid.NewGuid():n}"[..16];

    [Fact]
    public async Task PushAsync_returns_NotFound_for_an_unknown_source()
    {
        var result = await _ingress.PushAsync(FreshName("nope"), [], partial: false);
        Assert.Equal(IngestOutcome.NotFound, result.Outcome);
    }

    [Fact]
    public async Task PushAsync_returns_WrongKind_for_a_generator_kind_source()
    {
        // The plan 008 W4 brief's explicit rule: pushing into a generator-kind source is 409 —
        // mixing a timer-driven rate with client pushes would make every counter unreconcilable.
        var name = FreshName("gen");
        await SeedSourceAsync(name, SourceKinds.Generator);

        var result = await _ingress.PushAsync(name, [new() { ["price"] = 1.0 }], partial: false);
        Assert.Equal(IngestOutcome.WrongKind, result.Outcome);
    }

    [Fact]
    public async Task PushAsync_accepts_a_valid_batch_and_status_reflects_depth_and_totals()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig { CapacityRows = 100, MaxBatchRows = 100 });

        var result = await _ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false);
        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(2, result.Accepted);
        Assert.Equal(0, result.Dropped);
        Assert.Equal(0, result.Invalid);

        var status = await _ingress.GetStatusAsync(name);
        Assert.NotNull(status);
        Assert.Equal(2, status!.DepthRows);
        Assert.Equal(2, status.TotalAccepted);
        Assert.Equal(100, status.CapacityRows);
    }

    [Fact]
    public async Task PushAsync_rejects_invalid_rows_as_Invalid_unless_partial()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig());

        var result = await _ingress.PushAsync(name, [new() { ["price"] = "not-a-number" }], partial: false);
        Assert.Equal(IngestOutcome.Invalid, result.Outcome);
        Assert.Single(result.RowErrors);
    }

    [Fact]
    public async Task PushAsync_partial_admits_valid_rows_and_reports_invalid_count()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig { CapacityRows = 10, MaxBatchRows = 10 });

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["price"] = 1.5 },
            new() { ["price"] = "bad" },
        };
        var result = await _ingress.PushAsync(name, rows, partial: true);

        Assert.Equal(IngestOutcome.Accepted, result.Outcome);
        Assert.Equal(1, result.Accepted);
        Assert.Equal(1, result.Invalid);
        Assert.Single(result.RowErrors);
    }

    [Fact]
    public async Task PushAsync_oversized_batch_returns_TooLarge()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig { CapacityRows = 10, MaxBatchRows = 1 });

        var result = await _ingress.PushAsync(name, [new() { ["price"] = 1.0 }, new() { ["price"] = 2.0 }], partial: false);
        Assert.Equal(IngestOutcome.TooLarge, result.Outcome);
    }

    [Fact]
    public async Task PushAsync_over_capacity_under_Reject_policy_returns_Overloaded_with_a_positive_RetryAfter()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig
        {
            Policy = IngressOverflowPolicy.Reject,
            CapacityRows = 1,
            MaxBatchRows = 1,
        });

        // Fill the one-row buffer first so the second push has no room (batchSize <= MaxBatchRows/
        // CapacityRows here, so IngressAdmission.Decide reaches the Reject branch, not TooLarge).
        var first = await _ingress.PushAsync(name, [new() { ["price"] = 1.0 }], partial: false);
        Assert.Equal(IngestOutcome.Accepted, first.Outcome);

        var second = await _ingress.PushAsync(name, [new() { ["price"] = 2.0 }], partial: false);
        Assert.Equal(IngestOutcome.Overloaded, second.Outcome);
        Assert.True(second.RetryAfterMs > 0);
    }

    [Fact]
    public async Task GetStatusAsync_returns_null_for_a_non_ingest_source()
    {
        var name = FreshName("gen");
        await SeedSourceAsync(name, SourceKinds.Generator);
        Assert.Null(await _ingress.GetStatusAsync(name));
    }

    [Fact]
    public async Task GetStatusAsync_returns_null_for_an_unknown_source()
    {
        Assert.Null(await _ingress.GetStatusAsync(FreshName("nope")));
    }

    [Fact]
    public async Task GetStatusAsync_reports_the_configured_shape_with_zeroed_counters_before_any_push()
    {
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig { CapacityRows = 42, MaxBatchRows = 7 });

        var status = await _ingress.GetStatusAsync(name);
        Assert.NotNull(status);
        Assert.Equal(42, status!.CapacityRows);
        Assert.Equal(7, status.MaxBatchRows);
        Assert.Equal(0, status.DepthRows);
        Assert.Equal(0, status.TotalAccepted);
    }

    [Fact]
    public async Task Push_publishes_through_the_same_stream_identity_GeneratorGrain_and_ConnectorGrain_use()
    {
        // Inline policy publishes synchronously inside PushAsync, so subscribing first and awaiting the
        // delivery proves the drain pump reaches ("sources", sourceName) — the exact stream identity
        // pipelines/tables/SignalR/gRPC StreamService already subscribe to for every other source kind.
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig { Policy = IngressOverflowPolicy.Inline, MaxBatchRows = 10 });

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));

        var tcs = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handle = await stream.SubscribeAsync((evt, _) =>
        {
            tcs.TrySetResult(evt);
            return Task.CompletedTask;
        });

        try
        {
            var result = await _ingress.PushAsync(name, [new() { ["price"] = 9.5 }], partial: false);
            Assert.Equal(IngestOutcome.Accepted, result.Outcome);

            var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(name, received["_source"]);
            Assert.Equal(9.5, received["price"]);
        }
        finally
        {
            await handle.UnsubscribeAsync();
        }
    }

    [Fact]
    public async Task ConnectorStatusFacade_returns_null_for_an_ingest_kind_source_instead_of_activating_a_ConnectorGrain()
    {
        // The latent defect this wave fixes: before, only "generator" short-circuited this facade, so
        // any other Kind (including the new "ingest") fell through to a live IConnectorGrain call.
        var name = FreshName("ing");
        await SeedSourceAsync(name, SourceKinds.Ingest, new IngestConfig());

        Assert.Null(await _connectorStatus.GetStatusAsync(name));
    }
}
