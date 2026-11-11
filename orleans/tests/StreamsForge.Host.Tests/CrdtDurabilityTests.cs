using System.Collections.Concurrent;
using System.Text.Json;
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
using Xunit;
using Ycs;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Test-only grain storage keyed by the grain's string primary key (<c>GrainId.Key</c>, not the full
/// composite <c>GrainId</c>), so a test can pre-seed a raw JSON blob before the grain's first activation —
/// see <see cref="CrdtDurabilityTests.UpgradeIsANoOpForAWaveBShapedState"/>, which needs exactly that hook
/// to hand-build a state shaped like what wave B actually persisted (no "pendingUpdates" property at
/// all). Mirrors the pattern ApprovalGrainTests' RecordingGrainStorage already establishes in this
/// folder; not shared with it — its keying is by the full composite GrainId string via a "Contains" scan,
/// which works there but is more machinery than this file needs since every test here picks its own
/// globally-unique grain name already.
/// </summary>
internal static class DurabilityGrainStorageSeeds
{
    public static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> ByTest = new();

    public static ConcurrentDictionary<string, string> For(string testId) =>
        ByTest.GetOrAdd(testId, _ => new ConcurrentDictionary<string, string>());
}

internal sealed class DurabilitySeedableGrainStorage(string testId) : IGrainStorage
{
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var key = grainId.Key.ToString() ?? "";
        if (DurabilityGrainStorageSeeds.For(testId).TryGetValue(key, out var json))
        {
            var value = JsonSerializer.Deserialize<T>(json);
            if (value is not null)
            {
                grainState.State = value;
                grainState.RecordExists = true;
                grainState.ETag = "1";
                return Task.CompletedTask;
            }
        }

        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        DurabilityGrainStorageSeeds.For(testId)[grainId.Key.ToString() ?? ""] = JsonSerializer.Serialize(grainState.State);
        grainState.RecordExists = true;
        grainState.ETag = "1";
        return Task.CompletedTask;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        DurabilityGrainStorageSeeds.For(testId).TryRemove(grainId.Key.ToString() ?? "", out _);
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }
}

internal sealed class DurabilityTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.ConfigureServices(services => services.AddGrainStorage(
            StreamConstants.StorageName,
            (sp, _) => new DurabilitySeedableGrainStorage(
                sp.GetRequiredService<IConfiguration>()["TestId"]
                    ?? throw new InvalidOperationException("TestId not configured — see DurabilityTestSiloConfigurator."))));
    }
}

internal sealed class DurabilityTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 020 wave C — durability hazard tests for <c>CrdtDocGrain</c>'s snapshot+log compaction, each
/// pinning one hazard named in the wave brief. Uses its own silo/storage scaffolding (above) rather than
/// <see cref="CrdtDocGrainClusterTests"/>' because hazard 2 needs to pre-seed a raw JSON blob before the
/// grain's first activation — plain memory grain storage gives no such hook, and
/// <see cref="CrdtDocGrainClusterTests"/> is left untouched (not this wave's file to restructure).
///
/// <para>Hazards 3 (the generation guard covering the log append) and 4 (idempotent replay growing the
/// log with harmless no-ops) are handled in code and documented at their site in <c>CrdtDocGrain</c>
/// rather than pinned with a dedicated test here — the wave brief only requires an explicit test for
/// hazards 1, 2 and 5; see the wave report's "not covered by a test" section for why 3 specifically
/// resists a live reproduction (same reasoning <c>ConnectorGrain</c>'s own generation guard already lives
/// with, untested, for the identical reason).</para>
/// </summary>
public sealed class CrdtDurabilityTests : IAsyncLifetime
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
        builder.AddSiloBuilderConfigurator<DurabilityTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<DurabilityTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition MakeCrdtSource(string name) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Enabled = true,
        Fields =
        [
            new FieldDef("id", FieldType.String),
            new FieldDef("name", FieldType.String),
        ],
        Connector = new ConnectorConfig
        {
            Crdt = new CrdtSourceConfig { RootMap = "root", KeyField = "id" },
        },
    };

    private static YMap Root(YDoc doc) => doc.GetMap("root");

    // ------------------------------------------------------------------
    // Hazard 1 — a frame that failed to apply must never enter the log: force deactivation/reactivation
    // after a batch containing one corrupt update, and assert the document still activates (were the
    // corrupt bytes in the log, RehydrateDoc would rethrow decoding them on EVERY future activation — a
    // permanent denial of service on this grain) and still holds the good updates.
    // ------------------------------------------------------------------
    [Fact]
    public async Task ACorruptFrameNeverEntersTheLogSoReactivationStillWorks()
    {
        var name = "crdt_hz1_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var goodUpdate = edge.EncodeStateAsUpdateV1();

            // Not a legal v1 update frame — Ycs's decoder is expected to throw decoding it.
            var corrupt = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

            var batch = await grain.MergeAsync([goodUpdate, corrupt]);
            Assert.Equal(1, batch.UpdatesApplied);
            Assert.Equal(1, batch.RowsEmitted);
            Assert.Contains(batch.Diagnostics, d => d.Contains("update[1]") && d.Contains("skipped"));

            // Real deactivation, exactly the mechanism CrdtDocGrainClusterTests' own resume test uses.
            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            // If the corrupt bytes had reached the durable log, OnActivateAsync's RehydrateDoc call would
            // throw here (and on every subsequent call, forever) instead of returning a status.
            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);
            Assert.Equal(1, status.UpdatesMerged);

            // And the resumed document genuinely holds e1 (not merely "didn't throw") — D7: replaying the
            // update it already has emits nothing.
            var replay = await grain.MergeAsync([goodUpdate]);
            Assert.Equal(0, replay.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // Hazard 2 — upgrade must be a no-op: a state file written by wave B (full DocBytes, no log
    // property at all) must read back byte-identically through the new RehydrateDoc.
    // ------------------------------------------------------------------
    [Fact]
    public async Task UpgradeIsANoOpForAWaveBShapedState()
    {
        var name = "crdt_hz2_" + Guid.NewGuid().ToString("n")[..8];
        var def = MakeCrdtSource(name);

        var edge = new YDoc();
        var e1 = new YMap();
        Root(edge).Set("e1", e1);
        e1.Set("name", "Ann");
        var fullDocBytes = edge.EncodeStateAsUpdateV1();

        // Exactly wave B's persisted shape: DocBytes = the whole document, NO "pendingUpdates" property
        // anywhere in the JSON (wave B's CrdtDocGrainState never had that field). Hand-built, not
        // round-tripped through the current class, so a rename or a required-field default couldn't hide
        // a real gap here.
        var waveBJson = $$"""
            {"Def":{{JsonSerializer.Serialize(def)}},"Running":true,"DocBytes":"{{Convert.ToBase64String(fullDocBytes)}}","UpdatesMerged":1,"RowsEmittedTotal":1}
            """;
        DurabilityGrainStorageSeeds.For(_testId)[name] = waveBJson;

        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            // No StartAsync call — OnActivateAsync resumes straight from the seeded state, exactly like a
            // real silo waking a grain backed by a pre-existing wave B state file.
            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);
            Assert.Equal(1, status.UpdatesMerged);
            Assert.Equal(1, status.RowsEmitted);

            // D7 against the upgraded document: replaying the update it already contains emits nothing —
            // only true if RehydrateDoc actually applied DocBytes, not an empty doc.
            var replay = await grain.MergeAsync([fullDocBytes]);
            Assert.Equal(0, replay.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // Hazard 5 — compaction must not change what the document says: force at least one compaction (32+
    // accepted updates, CrdtDocGrain.CompactionEntryThreshold), then assert the projection after a
    // reactivation is identical to before.
    // ------------------------------------------------------------------
    [Fact]
    public async Task CompactionDoesNotChangeTheDocumentAfterReactivation()
    {
        var name = "crdt_hz5_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            // 40 crosses CrdtDocGrain.CompactionEntryThreshold (32) at least once, so this run genuinely
            // exercises CompactLog/UpdateOperations.MergeUpdates, not just the plain log-append path.
            for (var i = 0; i < 40; i++)
            {
                var entity = new YMap();
                Root(edge).Set($"e{i}", entity);
                entity.Set("name", $"n{i}");
                var result = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
                Assert.Equal(1, result.RowsEmitted);
            }

            var beforeStatus = await grain.GetStatusAsync();
            Assert.Equal(40, beforeStatus.EntityCount);

            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            var afterStatus = await grain.GetStatusAsync();
            Assert.Equal(beforeStatus.EntityCount, afterStatus.EntityCount);
            Assert.Equal(beforeStatus.UpdatesMerged, afterStatus.UpdatesMerged);
            Assert.Equal(beforeStatus.RowsEmitted, afterStatus.RowsEmitted);

            // D7 once more, post-compaction: the resumed, compacted document must still recognize an
            // already-applied edit as a no-op — proof the compacted snapshot says the same thing the
            // pre-compaction snapshot+log did, not just that the entity count matches.
            var replay = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
            Assert.Equal(0, replay.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 6 — the gap idempotence creates, and the only thing that closes it.
    //
    // Found by running wave C's own acceptance check live rather than in-process: after a kill -9 and a
    // restart the DOCUMENT came back perfectly (entityCount unchanged) and the TABLE came back EMPTY and
    // stayed rebuilding:true. TableGrain's own RESTART-RESUME LIMITATION resets a resuming table to empty
    // and rebuilds it "purely from live traffic going forward" — which works for a generator or a broker
    // and cannot work for a document, because D7 guarantees that re-sending the update history that
    // produced the state emits NOTHING. Re-asserting current state is the only refill there is.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ReplayReAssertsEveryLiveEntityBecauseReSendingUpdatesCannotRefillAConsumer()
    {
        var name = "crdt_replay_" + Guid.NewGuid().ToString("n")[..8];

        var stream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        var handle = await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) { received.Add(evt); }
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            edge.Transact(_ =>
            {
                var a = new YMap(); Root(edge).Set("a", a); a.Set("name", "Ann");
                var b = new YMap(); Root(edge).Set("b", b); b.Set("name", "Bob");
            });
            Assert.Equal(2, (await grain.MergeAsync([edge.EncodeStateAsUpdateV1()])).RowsEmitted);

            // Delete one, so the replay below has something it must NOT resurrect.
            edge.Transact(_ => Root(edge).Delete("b"));
            Assert.Equal(1, (await grain.MergeAsync([edge.EncodeStateAsUpdateV1()])).RowsEmitted);

            // The premise: re-sending the whole history emits nothing at all (D7). This is what makes a
            // lost-rows consumer unrecoverable without ReplayAsync, so it is asserted here rather than
            // assumed from the wave B test that pins it separately.
            Assert.Equal(0, (await grain.MergeAsync([edge.EncodeStateAsUpdateV1()])).RowsEmitted);

            // Stream delivery is asynchronous: drain the three pre-replay rows BEFORE clearing, or they
            // land after the clear and get counted as the replay's own output.
            await WaitUntilAsync(() => { lock (received) { return received.Count >= 3; } });
            lock (received) { received.Clear(); }

            var replay = await grain.ReplayAsync();

            Assert.Equal(1, replay.RowsEmitted);      // 'a' only
            Assert.Equal(0, replay.UpdatesApplied);   // merged nothing — it only read the document

            await WaitUntilAsync(() => { lock (received) { return received.Count >= 1; } });
            lock (received)
            {
                Assert.Single(received);
                var row = received[0];
                Assert.Equal("a", row["id"]);
                Assert.Equal("Ann", row["name"]);
                // A re-assert, not a delta: +1 create, so a LATEST BY consumer converges on it.
                Assert.Equal("c", row["_op"]);
                Assert.Equal(1L, row["_weight"]);
                Assert.False(row.ContainsKey("_retract"));
                Assert.Equal(name, row["_source"]);
            }

            // Calling it twice is harmless — it is a re-assert of the same state, not an increment.
            var second = await grain.ReplayAsync();
            Assert.Equal(1, second.RowsEmitted);

            // ...and it did not touch the document: nothing merged, entity count unchanged.
            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);
        }
        finally
        {
            await handle.UnsubscribeAsync();
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task ReplayingAStoppedDocumentSaysSoInsteadOfLookingLikeAnEmptyDocument()
    {
        // Same failure shape wave B found on MergeAsync: a bare "0 rows" is indistinguishable from "this
        // document is genuinely empty", and the caller issued a replay precisely because something
        // downstream was empty. It has to say which one it is.
        var name = "crdt_replay_stopped_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);

        await grain.StartAsync(MakeCrdtSource(name));
        await grain.StopAsync();

        var result = await grain.ReplayAsync();

        Assert.Equal(0, result.RowsEmitted);
        Assert.Contains(result.Diagnostics, d => d.Contains("not running", StringComparison.Ordinal));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 100 && !condition(); i++)
        {
            await Task.Delay(50);
        }
    }
}
