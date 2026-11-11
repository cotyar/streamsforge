using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using StreamsForge.Host.Storage;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Test-only IGrainStorage decorator wrapping a real JsonFileGrainStorage. Exists so
/// TablePersistenceModeClusterTests can assert on persistence-mode behavior deterministically instead of via
/// wall-clock races:
///  - AttemptsFor/CompletedFor(stateName) count real WriteStateAsync calls per Orleans state name ("table"
///    for TableGrain — see its [PersistentState("table", ...)] attribute), so "MemoryOnly never writes" and
///    "single-flight doesn't overlap" can be asserted as exact counts, not "probably didn't happen in time".
///  - HoldWrites()/ReleaseWrites() lets a test artificially keep one specific write in flight for as long as
///    it wants, to prove (a) the grain turn is NOT blocked behind it (a normal grain call issued while the
///    write is held must still return promptly) and (b) a later flush tick is skipped, not queued/overlapped,
///    while it's held (single-flight).
/// Only "table"/"tableHistory" writes are ever gated — RegistryGrain's own "catalog" writes (which happen as
/// a side effect of CreateTableAsync/SetTableStatusAsync themselves) are never held, or a held gate would
/// deadlock the very calls the tests use to set up/tear down each scenario.
/// </summary>
internal sealed class DelayableGrainStorage(IGrainStorage inner, params string[] gatedStateNames) : IGrainStorage
{
    private readonly HashSet<string> _gatedStateNames = new(gatedStateNames, StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _attempts = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _completed = new(StringComparer.Ordinal);
    private TaskCompletionSource<bool>? _hold;

    /// <summary>From this call on, every WriteStateAsync for a gated state name blocks until ReleaseWrites().</summary>
    public void HoldWrites() => _hold = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    public void ReleaseWrites() => _hold?.TrySetResult(true);

    public int AttemptsFor(string stateName) => _attempts.GetValueOrDefault(stateName);
    public int CompletedFor(string stateName) => _completed.GetValueOrDefault(stateName);

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ReadStateAsync(stateName, grainId, grainState);

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        _attempts.AddOrUpdate(stateName, 1, (_, c) => c + 1);

        var hold = _gatedStateNames.Contains(stateName) ? _hold : null;
        if (hold is not null)
        {
            await hold.Task;
        }

        await inner.WriteStateAsync(stateName, grainId, grainState);
        _completed.AddOrUpdate(stateName, 1, (_, c) => c + 1);
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ClearStateAsync(stateName, grainId, grainState);
}

/// <summary>Keyed registry bridging a test instance to the DelayableGrainStorage wired into ITS OWN silo:
/// ISiloConfigurator implementations are instantiated by Orleans TestingHost via reflection, with no direct
/// path back to the test object, so each test passes a random id through host configuration ("TestId") and
/// looks its own storage instance back up here once the cluster is deployed. Concurrently-running tests never
/// collide since each picks its own id.</summary>
internal static class PersistenceModeTestRegistry
{
    public static readonly ConcurrentDictionary<string, DelayableGrainStorage> Storages = new(StringComparer.Ordinal);
}

internal sealed class PersistenceModeTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.ConfigureServices(services =>
        {
            services.AddGrainStorage(StreamConstants.StorageName, (sp, providerName) =>
            {
                var config = sp.GetRequiredService<IConfiguration>();
                var testId = config["TestId"] ?? throw new InvalidOperationException("TestId not configured for this silo — see PersistenceModeTestSiloConfigurator.");
                var dataDir = config["DataDir"] ?? throw new InvalidOperationException("DataDir not configured for this silo — see PersistenceModeTestSiloConfigurator.");
                var real = new JsonFileGrainStorage(providerName, dataDir);
                var wrapped = new DelayableGrainStorage(real, "table", "tableHistory");
                PersistenceModeTestRegistry.Storages[testId] = wrapped;
                return wrapped;
            });
        });
    }
}

internal sealed class PersistenceModeTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>Plan 008 W2.5: per-table persistence mode (TableDefinition.Persistence/FlushMs). A real Orleans
/// TestingHost cluster, wired with the REAL JsonFileGrainStorage (wrapped by DelayableGrainStorage — see its
/// own doc comment) instead of the memory-storage other cluster test files use, since these tests need to
/// observe real file-write behavior (or its deliberate absence) on disk. Mirrors HistoryGrainClusterTests'/
/// PartitionedTableClusterTests' SeedTradesTableAsync/PublishTradeAsync/PollUntilAsync patterns.</summary>
public sealed class TablePersistenceModeClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _testId = null!;
    private string _dataDir = null!;
    private DelayableGrainStorage _storage = null!;

    public async Task InitializeAsync()
    {
        _testId = Guid.NewGuid().ToString("n");
        _dataDir = Path.Combine(Path.GetTempPath(), "sf-persistence-tests", _testId);
        Directory.CreateDirectory(_dataDir);

        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TestId"] = _testId,
            ["DataDir"] = _dataDir,
        }));
        builder.AddSiloBuilderConfigurator<PersistenceModeTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<PersistenceModeTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // AddGrainStorage registers a FACTORY, not an eagerly-constructed instance — Orleans only invokes it
        // lazily, the first time some grain using StreamConstants.StorageName actually activates. DeployAsync
        // alone doesn't activate any grain, so force one now (RegistryGrain, which every test's
        // SeedTradesTableAsync/CreateTableAsync call would activate anyway) before looking the instance up.
        await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetTablesAsync();
        _storage = PersistenceModeTestRegistry.Storages[_testId];
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        PersistenceModeTestRegistry.Storages.TryRemove(_testId, out _);
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Mirrors HistoryGrainClusterTests.SeedTradesTableAsync: creates a stopped test source, swaps
    /// it into the given TableDefinition's SQL ("__SOURCE__" placeholder), creates the table, and starts it.</summary>
    private async Task<(IRegistryGrain Registry, string SourceName, TableDefinition Created)> SeedTradesTableAsync(TableDefinition tableDef)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "trades_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });
        tableDef.Sql = tableDef.Sql.Replace("__SOURCE__", sourceName);
        var created = await registry.CreateTableAsync(tableDef);
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return (registry, sourceName, created);
    }

    private async Task PublishTradeAsync(string sourceName, string symbol, double price, long qty)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
            ["qty"] = qty,
        });
    }

    /// <summary>JsonFileGrainStorage's own naming convention (see its PathFor): "{stateName}.{sanitized
    /// grainId}.json" under "{dataDir}/state/". Rather than reproduce Orleans' exact grain-id-to-string
    /// encoding, this just looks for any "table.*" file whose name contains the (alphanumeric/underscore)
    /// table name — reliable given every test table name here is GUID-suffixed and contains no characters
    /// JsonFileGrainStorage's sanitizer would strip.</summary>
    private bool AnyPersistedTableFileFor(string tableName)
    {
        var stateDir = Path.Combine(_dataDir, "state");
        return Directory.Exists(stateDir) && Directory.GetFiles(stateDir, $"table.*{tableName}*.json").Length > 0;
    }

    /// <summary>Same naming-convention reasoning as AnyPersistedTableFileFor, for TableHistoryGrain's own
    /// [PersistentState("tableHistory", ...)] state name.</summary>
    private bool AnyPersistedHistoryFileFor(string tableName)
    {
        var stateDir = Path.Combine(_dataDir, "state");
        return Directory.Exists(stateDir) && Directory.GetFiles(stateDir, $"tableHistory.*{tableName}*.json").Length > 0;
    }

    [Fact]
    public async Task Batched_Default_RestartsAndResumesFromPersistedSnapshot_ByteIdenticalToPre008()
    {
        var tableName = "persist_batched_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            // Persistence defaults to Batched, FlushMs defaults to 0 (-> the pre-008 hardcoded 2000ms) —
            // this test deliberately leaves both at their defaults to prove that default path is unchanged.
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);
        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var rowCountSeen = await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        Assert.Equal(1, rowCountSeen);

        // StopAsync's own final-flush (dirty-gated, awaited) persists synchronously regardless of the
        // 2000ms interval, exactly as pre-008 — no need to wait out the periodic timer for this assertion.
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        Assert.True(AnyPersistedTableFileFor(tableName), "Batched table should have a persisted snapshot on disk after stop");
        Assert.True(_storage.CompletedFor("table") >= 1);

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        // Same documented restart-resume contract as pre-008 (TableGrain's class doc; see also
        // PartitionedTableClusterTests' Parallelism==1 analogue): resume marks Rebuilding and resets to
        // empty, rebuilding purely from live traffic going forward — Batched must still exhibit this
        // byte-identical behavior with FlushMs left at its default.
        var justRestarted = await tableGrain.GetMetricsAsync();
        Assert.True(justRestarted.Rebuilding);
        Assert.Equal(0, justRestarted.RowCount);

        await PublishTradeAsync(sourceName, "MSFT", 50, 3);
        var rowCountAfterRestart = await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        Assert.Equal(1, rowCountAfterRestart);
        var rows = await tableGrain.GetRowsAsync(10, 0);
        Assert.Contains(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "MSFT");
        Assert.DoesNotContain(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "AAPL");
    }

    [Fact]
    public async Task MemoryOnly_NeverWritesOnAnyPath_RestartYieldsEmptyTable()
    {
        var tableName = "persist_memonly_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.MemoryOnly,
            FlushMs = 50, // deliberately tiny: proves it's IGNORED (no timer registered at all — see
                          // TableGrain's StartClassicAsync/StartCoordinatorAsync), not just rarely firing.
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);
        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var rowCountSeen = await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        Assert.Equal(1, rowCountSeen);

        // Give a would-be 50ms timer several chances to fire (it was never registered) before asserting the
        // negative — this is a defense-in-depth wait, not the actual proof: the real proof is structural
        // (no timer exists), so the assertion below is not a race against "maybe it hasn't happened yet".
        await Task.Delay(500);
        Assert.Equal(0, _storage.AttemptsFor("table"));
        Assert.False(AnyPersistedTableFileFor(tableName));

        // Explicit stop (final-flush path) must ALSO skip — MemoryOnly's contract per TablePersistenceMode's
        // own doc comment: "nothing touches storage on any path".
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        Assert.Equal(0, _storage.AttemptsFor("table"));
        Assert.False(AnyPersistedTableFileFor(tableName));

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        var afterRestart = await tableGrain.GetMetricsAsync();
        Assert.Equal(0, afterRestart.RowCount);
        Assert.False(afterRestart.Rebuilding); // nothing was ever persisted to "rebuild" from — documented, not a bug.
    }

    [Fact]
    public async Task FireAndForget_DoesNotBlockTheTurn_SingleFlightSkipsOverlap_EventuallyPersists()
    {
        var tableName = "persist_faf_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.FireAndForget,
            FlushMs = 100,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);
        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        // Hold every "table" write open BEFORE any delta arrives — no tick has anything to flush yet, so
        // the FIRST write attempt observed below is deterministically the one this test controls (avoids a
        // race where an earlier, un-gated tick could complete before HoldWrites() runs).
        _storage.HoldWrites();
        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        // Wait for the flush timer to attempt a write — proves the tick captured the snapshot and
        // dispatched the write (single-flight's _pendingWrite is now in flight).
        await PollUntilAsync(() => Task.FromResult(_storage.AttemptsFor("table")), c => c >= 1, deadlineSeconds: 5);
        Assert.Equal(0, _storage.CompletedFor("table")); // still gated — the write hasn't reached disk yet

        // Non-blocking-turn proof: an ordinary grain call must return promptly even while a write is stuck.
        // If the write were awaited inside the turn (Batched-style), this call would itself queue behind it
        // on Orleans' single-threaded turn scheduler and stall until ReleaseWrites() below. RowCount is
        // already correct at this point too — CaptureSnapshotIntoState runs synchronously on the turn,
        // updating state.State.Snapshot, BEFORE the (gated) disk write is even dispatched.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var countWhileGated = await tableGrain.GetRowCountAsync();
        sw.Stop();
        Assert.Equal(1, countWhileGated);
        Assert.True(sw.ElapsedMilliseconds < 3000, $"GetRowCountAsync took {sw.ElapsedMilliseconds}ms while a write was gated — the turn appears blocked on it");

        // Single-flight proof: let several more flush intervals elapse while still gated — AttemptsFor
        // ("table") must stay at 1 (later ticks skipped outright, never queued/overlapped behind the one
        // still in flight).
        await Task.Delay(400); // ~4 more 100ms intervals
        Assert.Equal(1, _storage.AttemptsFor("table"));

        // Release the held write -> it completes, and the snapshot eventually reaches disk.
        _storage.ReleaseWrites();
        await PollUntilAsync(() => Task.FromResult(_storage.CompletedFor("table")), c => c >= 1, deadlineSeconds: 5);
        Assert.True(AnyPersistedTableFileFor(tableName));

        // A later tick can attempt again post-release — proves the grain isn't permanently wedged by the
        // earlier hold (single-flight only skips WHILE a write is in flight, not forever).
        await PublishTradeAsync(sourceName, "MSFT", 50, 3);
        await PollUntilAsync(() => Task.FromResult(_storage.AttemptsFor("table")), c => c >= 2, deadlineSeconds: 5);
    }

    [Fact]
    public async Task FlushMs_IsThreadedThroughToTheTimer_NotJustAcceptedAndIgnored()
    {
        var tableName = "persist_flushms_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Batched,
            FlushMs = 100,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);
        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var rowCountSeen = await PollUntilAsync(() => tableGrain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        Assert.Equal(1, rowCountSeen);

        // With a 100ms configured interval, the PERIODIC timer (not the stop-path final flush, which this
        // test never triggers) should persist well within a few seconds — proves FlushMs is actually
        // threaded through to RegisterGrainTimer's own interval, not just accepted and ignored (the pre-008
        // hardcoded 2000ms would still eventually pass this poll, but far more slowly — the point of the
        // assertion is that it happens fast, well under the hardcoded default).
        await PollUntilAsync(() => Task.FromResult(AnyPersistedTableFileFor(tableName)), found => found, deadlineSeconds: 5);
    }

    [Fact]
    public async Task TableHistoryGrain_MemoryOnly_NeverWritesHistory_ButReadsStayLiveAndZeroLag()
    {
        var tableName = "persist_hist_memonly_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.MemoryOnly,
            FlushMs = 50,
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);
        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);

        // ResetAsync (called once from CreateTableAsync) always does one unawaited-by-nothing, mode-agnostic
        // config write (HistoryEnabled/HistoryMode/etc, with empty Entries) — a deliberate scope decision
        // (see TableHistoryGrain's class doc): that ONE-TIME write is not part of the periodic dirty-flag +
        // timer flush loop this feature targets, so it's expected regardless of Persistence. What MemoryOnly
        // must actually prevent is any FURTHER write — from the periodic timer (never registered) or from
        // the stop/deactivate path — as data keeps accumulating in _liveEntries.
        var baselineAttempts = _storage.AttemptsFor("tableHistory");

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        // Reads must stay live/zero-lag (TableHistoryGrain's own class doc's persistence-mode paragraph —
        // _liveEntries, not state.State.Entries, backs GetHistoryAsync/GetStatsAsync) even though nothing
        // further is ever persisted for this table's history in MemoryOnly.
        var result = await PollUntilAsync(() => historyGrain.GetHistoryAsync(key, 0), r => r.KeyFound, deadlineSeconds: 15);
        Assert.True(result.KeyFound);

        await Task.Delay(500); // several would-be 50ms intervals — the timer was never registered at all
        Assert.Equal(baselineAttempts, _storage.AttemptsFor("tableHistory"));

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        Assert.Equal(baselineAttempts, _storage.AttemptsFor("tableHistory")); // deactivate/stop path also skips
    }

    [Fact]
    public async Task TableHistoryGrain_FireAndForget_SingleFlight_DoesNotOverlap_AndLiveReadsStayFreshWhileWriteIsHeld()
    {
        var tableName = "persist_hist_faf_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.FireAndForget,
            FlushMs = 100,
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(def);
        var historyGrain = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(tableName);
        var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = "AAPL" }, ["symbol"]);

        // ResetAsync's own one-time config write (see the MemoryOnly test above) already completed by now —
        // baseline both counts before gating, so the poll below can't be satisfied by that earlier write.
        var baselineAttempts = _storage.AttemptsFor("tableHistory");
        var baselineCompleted = _storage.CompletedFor("tableHistory");

        _storage.HoldWrites();
        await PublishTradeAsync(sourceName, "AAPL", 100, 10);

        await PollUntilAsync(() => Task.FromResult(_storage.AttemptsFor("tableHistory")), c => c >= baselineAttempts + 1, deadlineSeconds: 5);
        Assert.Equal(baselineCompleted, _storage.CompletedFor("tableHistory")); // still gated

        // _liveEntries-backed reads must stay fresh WHILE the stale capture is stuck mid-write — this is the
        // whole point of decoupling _liveEntries from state.State.Entries (see TableHistoryGrain's class
        // doc): more deltas arriving during the held write must still show up immediately via GetHistoryAsync.
        await PublishTradeAsync(sourceName, "AAPL", 200, 5);
        var result = await PollUntilAsync(() => historyGrain.GetHistoryAsync(key, 0), r => r.KeyFound && r.TotalVersions >= 2, deadlineSeconds: 5);
        Assert.True(result.KeyFound);
        Assert.True(result.TotalVersions >= 2);

        // Single-flight: additional flush intervals elapsing while still gated must not push AttemptsFor
        // past baseline+1.
        await Task.Delay(400);
        Assert.Equal(baselineAttempts + 1, _storage.AttemptsFor("tableHistory"));

        _storage.ReleaseWrites();
        await PollUntilAsync(() => Task.FromResult(_storage.CompletedFor("tableHistory")), c => c >= baselineCompleted + 1, deadlineSeconds: 5);
        Assert.True(AnyPersistedHistoryFileFor(tableName));
    }

    [Fact]
    public async Task NegativeFlushMs_RejectedByValidation_OnCreateAndUpdate()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

        var createEx = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.CreateTableAsync(new TableDefinition
        {
            Name = "persist_negflush_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol, COUNT(*) AS trades FROM trades GROUP BY symbol",
            FlushMs = -1,
        }));
        Assert.Contains("FlushMs", createEx.Message);

        // Also rejected on update (of an otherwise-valid, previously-created table).
        var def = new TableDefinition
        {
            Name = "persist_negflush2_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol, COUNT(*) AS trades FROM trades GROUP BY symbol",
        };
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "trades",
            Enabled = false,
            EventsPerSecond = 0,
            Fields = [new FieldDef("symbol", FieldType.String)],
        });
        var created = await registry.CreateTableAsync(def);
        created.FlushMs = -100;
        var updateEx = await Assert.ThrowsAsync<InvalidOperationException>(() => registry.UpdateTableAsync(created));
        Assert.Contains("FlushMs", updateEx.Message);
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(100);
        }
        return last;
    }
}
