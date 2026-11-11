using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;
using Orleans.Core.Internal;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring <c>ConnectorTestSiloConfigurator</c> (ConnectorGrainClusterTests.cs) —
/// duplicated rather than shared, per that file's own "xunit test classes shouldn't share cluster state"
/// rationale.</summary>
internal sealed class PolledConnectorSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class PolledConnectorClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 014 wave E: the ORLEANS DRIVER half of the polled seam, on a real cluster. Where
/// <see cref="PolledTransportRegistryTests"/> drives <c>PolledSourceCore</c> directly — proving the cursor
/// rules in isolation — this file proves the part only a driver can: that a kind the repo has never heard
/// of gets a real grain timer, that its cursor reaches durable grain storage, and that a
/// <b>genuine deactivation</b> followed by a reactivation resumes from that storage rather than from a
/// still-warm activation.
///
/// <para><see cref="ADeactivatedConnectorResumesFromThePersistedCursor"/> is the test that justifies the
/// entire design. The plan's opening argument is that an <c>IAsyncEnumerable</c>-shaped SPI would keep the
/// cursor inside a subscription instance and lose it on every silo recycle; if that claim is not exercised
/// against an actual deactivation, nothing distinguishes this seam from the one it was written to avoid.
/// So the deactivation here is Orleans' own explicit one (<c>IGrainManagementExtension.DeactivateOnIdle</c>
/// — not idle collection, which a running connector is deliberately pinned against since bd147ce), and it
/// is <i>verified</i> before the resume is asserted: the transport records every poll, and the test proves
/// polling STOPPED across a window in which several scheduled cycles would otherwise have fired — the
/// grain timer dies with the activation, which is why the silence is evidence. Stated limit: this harness's
/// grain storage is <c>AddJsonMemoryGrainStorage</c> (System.Text.Json, production's format), which is
/// silo-host-scoped — it outlives an ACTIVATION, which is exactly
/// what is under test, but not a silo restart. Production's durable <c>AddJsonFileGrainStorage</c>
/// (Program.cs) covers the wider case, and no test here claims it.</para>
///
/// <para><b>Registration hygiene.</b> <see cref="PolledTransports"/> is process-global and permanent, so —
/// following <see cref="TransportRegistryTests"/> and <see cref="PolledTransportRegistryTests"/> — this
/// file registers exactly one distinctively-named kind, exactly once, from a static constructor. The kind
/// differs from those files' ("buzzdb" vs "fizzdb") because a duplicate registration is a throw, and the
/// two suites share a process. Per-test scripting therefore lives in a dictionary keyed by source name
/// rather than in the registered instance, so tests running in the same process cannot script each
/// other.</para>
/// </summary>
public sealed class ConnectorGrainPolledClusterTests : IAsyncLifetime
{
    private const string BuzzDbKind = "buzzdb";

    static ConnectorGrainPolledClusterTests() => PolledTransports.Register(new BuzzDb());

    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<PolledConnectorSiloConfigurator>();
        builder.AddClientBuilderConfigurator<PolledConnectorClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    // ------------------------------------------------------------------
    // The fake transport, and the per-source script it reads from
    // ------------------------------------------------------------------

    /// <summary>What one source's transport is told to do, and what it saw. Every member is guarded: the
    /// grain polls on its own turn while the test asserts from the xunit thread, so an unguarded list here
    /// would be a flaky test dressed up as a driver bug.</summary>
    private sealed class Script
    {
        private static readonly PolledBatch Starved = new([], null, false);

        private readonly Lock _gate = new();
        private readonly Queue<PolledBatch> _pages = new();
        private readonly List<string?> _seen = [];
        private Exception? _fail;

        /// <summary>Every cursor the driver handed over, in order — the evidence that what the grain
        /// persisted is what came back on the next cycle.</summary>
        public List<string?> SeenCursors
        {
            get { lock (_gate) return [.. _seen]; }
        }

        /// <summary>Set to make polls fail the way a real transport does: an exception out of the driver's
        /// timer callback, not a returned error code. Stays set until the test clears it, so a scheduled
        /// re-poll cannot silently "recover" between the failure and the assertion.</summary>
        public Exception? Fail
        {
            get { lock (_gate) return _fail; }
            set { lock (_gate) _fail = value; }
        }

        public void Enqueue(PolledBatch page)
        {
            lock (_gate) _pages.Enqueue(page);
        }

        /// <summary>An exhausted script returns an EMPTY batch rather than blocking: blocking would hold
        /// the grain turn and an activation Orleans is busy running is an activation it will not collect,
        /// which would quietly disarm the deactivation test below.</summary>
        public PolledBatch Next(string? cursor)
        {
            lock (_gate)
            {
                _seen.Add(cursor);
                return _fail is not null ? throw _fail
                    : _pages.Count > 0 ? _pages.Dequeue()
                    : Starved;
            }
        }
    }

    /// <summary>One registered instance for the whole process, dispatching by source name — see this
    /// class's registration-hygiene note. It rides the existing <c>connector.db</c> config slot for the
    /// same reason <c>TransportRegistryTests</c>' fake rides the nats one: adding an <c>[Id]</c> to
    /// <c>ConnectorConfig</c> for a test fixture would be a contract change.</summary>
    private sealed class BuzzDb : IPolledTransport
    {
        private static readonly Lock Gate = new();
        private static readonly Dictionary<string, Script> Scripts = new(StringComparer.Ordinal);

        public static Script ScriptFor(string sourceName)
        {
            lock (Gate)
            {
                if (!Scripts.TryGetValue(sourceName, out var script))
                {
                    Scripts[sourceName] = script = new Script();
                }
                return script;
            }
        }

        public string Kind => BuzzDbKind;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            if (string.IsNullOrWhiteSpace(def.Connector?.Db?.Table))
            {
                errors.Add("kind 'buzzdb' requires connector.db.table");
            }
        }

        public Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
            => Task.FromResult(ScriptFor(def.Name).Next(cursor));

        public TransportDescriptor Describe() => new()
        {
            Kind = BuzzDbKind,
            Label = "BuzzDB",
            ConfigProperty = "db",
            Polled = true,
            Mapping = false,
            Fields = [new TransportField { Key = "table", Label = "Table", Required = true }],
        };
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static string FreshName(string prefix) => $"{prefix}_{Guid.NewGuid():n}"[..20];

    private static SourceDefinition PolledSource(string name, int intervalMs = 1000, string dedupKeyColumn = "id") => new()
    {
        Name = name,
        Kind = BuzzDbKind,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = intervalMs },
            Db = new DbSourceConfig
            {
                Host = "buzz.local",
                Database = "warehouse",
                Table = "orders",
                CursorColumn = "updated_at",
                DedupKeyColumn = dedupKeyColumn,
            },
        },
    };

    private static Dictionary<string, object?> Row(string id, object? qty) => new() { ["id"] = id, ["qty"] = qty };

    private async Task<IAsyncDisposable> SubscribeAsync(string sourceName, List<EventRecord> sink)
    {
        var handle = await _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName))
            .SubscribeAsync((evt, _) =>
            {
                lock (sink) sink.Add(evt);
                return Task.CompletedTask;
            });
        return new Unsubscriber(handle);
    }

    private sealed class Unsubscriber(StreamSubscriptionHandle<EventRecord> handle) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { await handle.UnsubscribeAsync(); } catch { /* best-effort */ }
        }
    }

    private static List<EventRecord> Snapshot(List<EventRecord> sink)
    {
        lock (sink) return [.. sink];
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(150);
        }
        return last;
    }

    // ------------------------------------------------------------------
    // 1 — a registered polled kind gets a real schedule, and its rows are events
    // ------------------------------------------------------------------

    [Fact]
    public async Task APolledKindRunsOnItsScheduleAndItsRowsLandAsEvents()
    {
        var name = FreshName("buzz_rows");
        var script = BuzzDb.ScriptFor(name);
        script.Enqueue(new PolledBatch([Row("a1", "7"), Row("a2", 9L)], "c1", false));

        var sink = new List<EventRecord>();
        await using var _ = await SubscribeAsync(name, sink);

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(PolledSource(name));

            await PollUntilAsync(() => Task.FromResult(Snapshot(sink).Count), c => c >= 2);
            var first = Snapshot(sink);

            Assert.Equal(2, first.Count);
            Assert.All(first, evt => Assert.Equal(name, evt.Source));
            // Coerced and stamped by the SAME shared path url/file/folder rows go through — "7" arrives as
            // a string from the result set and lands as the declared Long.
            Assert.Equal(7L, first.Single(e => (string?)e.GetValueOrDefault("id") == "a1")["qty"]);
            Assert.Equal(9L, first.Single(e => (string?)e.GetValueOrDefault("id") == "a2")["qty"]);

            // The NEXT scheduled cycle re-reads a1 (what a `>=` cursor does to its own watermark) and adds
            // a3. DedupKeyColumn is read from connector.db by the driver — a polled source has no mapping
            // document for MappingSpec.DedupKeyField to come from — so a1 must be suppressed exactly once.
            script.Enqueue(new PolledBatch([Row("a1", 7L), Row("a3", 3L)], "c2", false));

            await PollUntilAsync(() => Task.FromResult(Snapshot(sink).Count), c => c >= 3);
            await Task.Delay(1500); // let any (incorrect) re-emission show up before counting

            var final = Snapshot(sink);
            Assert.Equal(3, final.Count);
            Assert.Single(final, e => (string?)e.GetValueOrDefault("id") == "a1");
            Assert.Contains(final, e => (string?)e.GetValueOrDefault("id") == "a3");
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 2 — the cursor advances, and every cycle is handed the one that was stored
    // ------------------------------------------------------------------

    [Fact]
    public async Task TheCursorAdvancesAcrossCyclesAndIsWhatTheNextCycleIsHanded()
    {
        var name = FreshName("buzz_cursor");
        var script = BuzzDb.ScriptFor(name);
        script.Enqueue(new PolledBatch([Row("b1", 1L)], "c1", false));
        script.Enqueue(new PolledBatch([Row("b2", 2L)], "c2", false));

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(PolledSource(name));

            var status = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor == "c2");
            Assert.Equal("c2", status.Cursor);
            Assert.Equal("ok", status.LastStatus);

            // An exhausted script returns an empty batch with a null cursor, which means "leave it
            // unchanged" and never "start over" — so the cursor must SURVIVE the idle cycles that follow.
            await PollUntilAsync(
                () => Task.FromResult(script.SeenCursors.Count), c => c >= 4, deadlineSeconds: 10);
            Assert.Equal("c2", (await grain.GetStatusAsync()).Cursor);

            // What was stored is exactly what the next cycle was handed — the whole point of persisting it
            // rather than keeping it in a subscription instance.
            var seen = script.SeenCursors;
            Assert.Equal([null, "c1", "c2"], seen.Take(3));
            Assert.All(seen.Skip(3), c => Assert.Equal("c2", c));
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 3 — THE test: a real deactivation, and a resume from durable storage
    // ------------------------------------------------------------------

    [Fact]
    public async Task ADeactivatedConnectorResumesFromThePersistedCursor()
    {
        var name = FreshName("buzz_resume");
        var script = BuzzDb.ScriptFor(name);
        script.Enqueue(new PolledBatch([Row("r1", 1L)], "resume-1", false));

        var sink = new List<EventRecord>();
        await using var _ = await SubscribeAsync(name, sink);

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(PolledSource(name));
            await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor == "resume-1");

            // Let the first row finish its trip down the stream before anything is collected. Emission and
            // delivery are not the same instant, and collecting the cluster out from under an in-flight
            // memory-stream item would be this test failing on Orleans' plumbing rather than on the cursor.
            await PollUntilAsync(() => Task.FromResult(Snapshot(sink).Count), c => c >= 1);

            // Deactivate for real, and the timer goes with the activation. Not ForceActivationCollection: a
            // RUNNING connector deliberately pins itself with DelayDeactivation(Infinite) since bd147ce (idle
            // collection must never stop a live source — SourceIdleCollectionTests), so collection leaves it
            // alone. An explicit deactivation is what a silo shutdown or a rebalance does to it.
            await grain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

            // Evidence that it really went: the 1s schedule would have produced several more polls across
            // this window. Silence is only possible if no activation is running the timer.
            var pollsAtCollection = script.SeenCursors.Count;
            await Task.Delay(3000);
            Assert.Equal(pollsAtCollection, script.SeenCursors.Count);

            // Now wake it with an ordinary grain call. OnActivateAsync sees persisted Running + a NextRunMs
            // already in the past, so it re-arms immediately — and the cycle that follows must be handed
            // the cursor read back out of grain storage by the NEW activation, not null (which would
            // re-read the source from the beginning) and not a value some surviving object still held.
            script.Enqueue(new PolledBatch([Row("r2", 2L)], "resume-2", false));
            var afterWake = await grain.GetStatusAsync();
            Assert.Equal("resume-1", afterWake.Cursor);

            var resumed = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor == "resume-2");
            Assert.Equal("resume-2", resumed.Cursor);

            Assert.Equal("resume-1", script.SeenCursors[pollsAtCollection]);
            await PollUntilAsync(() => Task.FromResult(Snapshot(sink).Count), c => c >= 2);
            Assert.Equal(["r1", "r2"], Snapshot(sink).Select(e => (string?)e.GetValueOrDefault("id")));
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 4 — a throwing transport surfaces an error and keeps its cursor
    // ------------------------------------------------------------------

    [Fact]
    public async Task AThrowingTransportKeepsTheCursorAndReportsThroughTheExistingStatusPath()
    {
        var name = FreshName("buzz_throw");
        var script = BuzzDb.ScriptFor(name);
        script.Enqueue(new PolledBatch([Row("t1", 1L)], "kept", false));

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(PolledSource(name));
            await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor == "kept");

            script.Fail = new InvalidOperationException("connection reset by peer");

            var failed = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.LastStatus == "error");

            // Nothing new about the error channel: the same LastStatus/LastError/ConsecutiveFailures the
            // url/file/folder kinds have always reported, driven by the same backoff.
            Assert.Equal("error", failed.LastStatus);
            Assert.Contains("connection reset by peer", failed.LastError);
            Assert.Contains(nameof(InvalidOperationException), failed.LastError);
            Assert.True(failed.ConsecutiveFailures >= 1);
            Assert.Equal(0, failed.LastBatchCount);

            // THE invariant: a transport bug must not be able to skip data. The rows behind "kept" stay
            // re-readable because the cursor never moved past them.
            Assert.Equal("kept", failed.Cursor);

            // …and the failed cycle asked from exactly there. (The recovery cycle is not asserted here: one
            // failure pushes the next run out by the D-E backoff, so waiting for it would buy a 30s test
            // for a rule PolledTransportRegistryTests already pins at the core.)
            Assert.Contains("kept", script.SeenCursors);
            Assert.Equal("kept", script.SeenCursors[^1]);
        }
        finally
        {
            script.Fail = null;
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 5 — HasMore pages a snapshot without waiting for the schedule
    // ------------------------------------------------------------------

    [Fact]
    public async Task HasMoreReArmsImmediatelyAndPersistsOneCursorPerPage()
    {
        // The discrimination is arithmetic: 8 pages on a 5s schedule is 40s of waiting if paging honors the
        // schedule, and roughly one cycle's latency if it re-arms immediately. The deadline below sits
        // between the two, so a regression that drops the re-arm cannot pass this by being slow-but-lucky.
        // The first cycle still waits out one interval — HasMore accelerates paging, it does not skip the
        // schedule — which is why the interval is 5s and not a minute.
        const int Pages = 8;
        var name = FreshName("buzz_pages");
        var script = BuzzDb.ScriptFor(name);
        for (var i = 1; i <= Pages; i++)
        {
            script.Enqueue(new PolledBatch([Row($"p{i}", i)], $"page-{i}", HasMore: i < Pages));
        }

        var sink = new List<EventRecord>();
        await using var _ = await SubscribeAsync(name, sink);

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            await grain.StartAsync(PolledSource(name, intervalMs: 5_000));

            var paged = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor == $"page-{Pages}", deadlineSeconds: 20);
            Assert.Equal($"page-{Pages}", paged.Cursor);

            // One durable cursor per page, in order, each one handed to the cycle that read the next page.
            Assert.Equal([null, "page-1", "page-2"], script.SeenCursors.Take(3));

            await PollUntilAsync(() => Task.FromResult(Snapshot(sink).Count), c => c >= Pages);
            Assert.Equal(
                Enumerable.Range(1, Pages).Select(i => $"p{i}"),
                Snapshot(sink).Select(e => (string?)e.GetValueOrDefault("id")));

            // The last page cleared HasMore, so the 5s schedule takes over again — no spinning.
            var settled = await grain.GetStatusAsync();
            Assert.True(
                settled.NextRunMs - settled.LastRunMs >= 4_000,
                $"expected the schedule to resume after the final page, got a {settled.NextRunMs - settled.LastRunMs}ms gap");
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 6 — the cursor is on the status record
    // ------------------------------------------------------------------

    [Fact]
    public async Task TheCursorIsSurfacedOnConnectorRuntimeStatus()
    {
        var name = FreshName("buzz_status");
        BuzzDb.ScriptFor(name).Enqueue(new PolledBatch([Row("s1", 1L)], "lsn/0/16B3748", false));

        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            // Null until there is one — an operator reading a source with no cursor must not see an
            // invented empty string.
            Assert.Null((await grain.GetStatusAsync()).Cursor);

            await grain.StartAsync(PolledSource(name));

            var status = await PollUntilAsync(() => grain.GetStatusAsync(), s => s.Cursor is not null);

            // Verbatim and opaque: an LSN goes through the platform untouched.
            Assert.Equal("lsn/0/16B3748", status.Cursor);
            Assert.Equal(name, status.SourceName);
        }
        finally
        {
            await grain.StopAsync();
        }
    }
}
