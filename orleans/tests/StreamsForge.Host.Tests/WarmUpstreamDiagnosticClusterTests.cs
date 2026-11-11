using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Captures every Warning-or-above log record emitted by a test cluster's silo, bucketed by the
/// <c>TestId</c> passed through host configuration — same "static registry keyed by TestId" idiom
/// <c>PersistenceModeTestRegistry</c> already uses to hand a per-test dependency across the
/// Orleans-owned configurator boundary (configurators are constructed by the generic host, not by test
/// code, so there is no constructor-injection seam to use instead).</summary>
internal static class WarmUpstreamTestRegistry
{
    public static readonly ConcurrentDictionary<string, ConcurrentQueue<string>> Warnings = new(StringComparer.Ordinal);
}

internal sealed class CapturingLoggerProvider(string testId) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new CapturingLogger(testId, categoryName);
    public void Dispose() { }
}

internal sealed class CapturingLogger(string testId, string categoryName) : ILogger
{
    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        var message = $"[{categoryName}] {formatter(state, exception)}";
        WarmUpstreamTestRegistry.Warnings.GetOrAdd(testId, _ => new ConcurrentQueue<string>()).Enqueue(message);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

internal sealed class WarmUpstreamTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
        siloBuilder.ConfigureServices(services =>
        {
            services.AddSingleton<ILoggerProvider>(sp =>
            {
                var config = sp.GetRequiredService<IConfiguration>();
                var testId = config["TestId"] ?? throw new InvalidOperationException("TestId not configured for this silo — see WarmUpstreamTestSiloConfigurator.");
                return new CapturingLoggerProvider(testId);
            });
        });
    }
}

internal sealed class WarmUpstreamTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Wishlist #14, option (b) — "at minimum a diagnostic at create time when an input LATEST BY table is
/// non-empty". This is a cluster-level test of <c>TableGrain.WarnIfTableInputsAlreadyHoldRowsAsync</c>
/// (Orleans classic/Parallelism==1 path): the loud, at-create-time log warning shipped INSTEAD of a real
/// backfill (option (a) — see that method's own doc comment for why: the classic path has no
/// snapshot/subscription synchronization point to do a real backfill against without a wire-contract
/// change to the (StreamConstants.TableDeltaNamespace, tableName) delta stream, which is out of scope for
/// this fix and touches files this change does not own).
///
/// Two angles: (1) a table started AFTER its table input already holds rows gets the warning, by name and
/// row count; (2) a table started the ordinary way (its table input still empty) gets NO such warning —
/// the diagnostic has to stay silent in the healthy case or it is noise, not a signal (same bar
/// TableExecutor.UnmatchedRetractions already holds itself to on the Engine side — see
/// AggregateOverWarmTableTests.A_table_with_no_group_by_reports_minus_one_rather_than_a_misleading_zero
/// and its -1/0 distinction).
/// </summary>
public sealed class WarmUpstreamDiagnosticClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _testId = null!;

    public async Task InitializeAsync()
    {
        _testId = Guid.NewGuid().ToString("n");
        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TestId"] = _testId,
        }));
        builder.AddSiloBuilderConfigurator<WarmUpstreamTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<WarmUpstreamTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        WarmUpstreamTestRegistry.Warnings.TryRemove(_testId, out _);
    }

    private async Task<string> SeedSourceAsync(IRegistryGrain registry)
    {
        var sourceName = "warm_src_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "warm-upstream diagnostic test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
            ],
        });
        return sourceName;
    }

    private async Task PublishTickAsync(string sourceName, string symbol, double price)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
        });
    }

    private async Task<int> PollRowCountAsync(ITableGrain grain, int atLeast, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        int last = 0;
        while (DateTime.UtcNow < deadline)
        {
            last = await grain.GetRowCountAsync();
            if (last >= atLeast) return last;
            await Task.Delay(50);
        }
        return last;
    }

    private IEnumerable<string> WarningsSoFar() =>
        WarmUpstreamTestRegistry.Warnings.TryGetValue(_testId, out var q) ? q.ToArray() : [];

    [Fact]
    public async Task TableStartedAfterItsTableInputAlreadyHoldsRows_LogsAWarningNamingTheUpstreamAndRowCount()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);

        var upstreamName = "warm_up_" + Guid.NewGuid().ToString("n")[..8];
        var upstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);
        var upstreamGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(upstreamName);

        // Warm the upstream BEFORE the consumer exists — the exact shape wishlist #14 reports (a scenario
        // cube / aggregate created mid-session over a long-since-populated table).
        await PublishTickAsync(sourceName, "AAPL", 100.0);
        await PublishTickAsync(sourceName, "MSFT", 200.0);
        await PublishTickAsync(sourceName, "GOOG", 50.0);
        var upstreamRows = await PollRowCountAsync(upstreamGrain, atLeast: 3, TimeSpan.FromSeconds(10));
        Assert.Equal(3, upstreamRows);

        // NOW create the consumer — this is the StartClassicAsync call that runs
        // WarnIfTableInputsAlreadyHoldRowsAsync BEFORE subscribing to the upstream's delta stream.
        var consumerName = "warm_agg_" + Guid.NewGuid().ToString("n")[..8];
        var consumer = await registry.CreateTableAsync(new TableDefinition
        {
            Name = consumerName,
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstreamName} GROUP BY symbol",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(consumer.Id, PipelineStatus.Running);
        var consumerGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(consumerName);

        // The table must still start normally despite the warning — advisory only, never blocking (same
        // "still starts" precedent as ApplyRetentionPolicy's own unsupported-policy warning).
        var deadline = DateTime.UtcNow.AddSeconds(10);
        PipelineStatus status;
        do
        {
            status = (await consumerGrain.GetMetricsAsync()).Status;
            if (status == PipelineStatus.Running) break;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        Assert.Equal(PipelineStatus.Running, status);

        // Give the warning a moment to land (logged synchronously inside StartClassicAsync, before the
        // grain call even returns to SetTableStatusAsync's caller — this poll is generous headroom only).
        List<string> matches = [];
        deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            matches = WarningsSoFar().Where(w => w.Contains(consumerName, StringComparison.Ordinal) && w.Contains(upstreamName, StringComparison.Ordinal)).ToList();
            if (matches.Count > 0) break;
            await Task.Delay(50);
        }

        Assert.NotEmpty(matches);
        Assert.Contains(matches, m => m.Contains("3", StringComparison.Ordinal)); // the row count it warned about
        Assert.Contains(matches, m => m.Contains("otc-demo-wishlist.md #14", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TableStartedTheOrdinaryWay_BeforeItsTableInputHasAnyRows_LogsNoWarmUpstreamWarning()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);

        var upstreamName = "cold_up_" + Guid.NewGuid().ToString("n")[..8];
        var upstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);

        // The consumer is created immediately, before the upstream has ever seen a row — the ordinary,
        // healthy startup order every table takes today.
        var consumerName = "cold_agg_" + Guid.NewGuid().ToString("n")[..8];
        var consumer = await registry.CreateTableAsync(new TableDefinition
        {
            Name = consumerName,
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstreamName} GROUP BY symbol",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(consumer.Id, PipelineStatus.Running);
        var consumerGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(consumerName);

        var deadline = DateTime.UtcNow.AddSeconds(10);
        PipelineStatus status;
        do
        {
            status = (await consumerGrain.GetMetricsAsync()).Status;
            if (status == PipelineStatus.Running) break;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        Assert.Equal(PipelineStatus.Running, status);

        // Now feed rows through — churn after attach is exactly what a table should see with no warning.
        await PublishTickAsync(sourceName, "AAPL", 100.0);
        await Task.Delay(500); // give any (wrongly-fired) warning a real chance to land before asserting its absence

        var falsePositives = WarningsSoFar().Where(w => w.Contains(consumerName, StringComparison.Ordinal)).ToList();
        Assert.Empty(falsePositives);
    }
}
