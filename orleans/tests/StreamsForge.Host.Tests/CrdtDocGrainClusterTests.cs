using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using Xunit;
using Ycs;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Silo config mirroring ConnectorGrainClusterTests'/ConnectorGrainPolledClusterTests' own
/// configurator (memory streams + memory grain storage) — duplicated here (not shared), same reason those
/// files give: xunit test classes shouldn't share cluster state.</summary>
internal sealed class CrdtTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class CrdtTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

/// <summary>
/// Plan 020 wave B-2: cluster tests for the Orleans CRDT document driver
/// (<see cref="ICrdtDocGrain"/>/<see cref="StreamsForge.Host.Grains.CrdtDocGrain"/>) against a real
/// <see cref="TestCluster"/>, mirroring <c>ConnectorGrainClusterTests</c>'/
/// <c>ConnectorGrainPolledClusterTests</c>' own harness shape. Every document here is a REAL
/// <see cref="YDoc"/> built through the Ycs API — the same "no mock where there is no socket" standard
/// <c>CrdtProjectorTests</c> already holds itself to.
///
/// <para><b>How a test document is produced.</b> Each test keeps ONE persistent "edge" <see cref="YDoc"/>
/// (simulating the disconnected client) and, after every edit, calls
/// <see cref="YDoc.EncodeStateAsUpdateV1"/> with NO target state vector — i.e. the edge's ENTIRE current
/// state, deletes included, not an incremental diff. Feeding successive full-state snapshots into the
/// SAME grain document converges exactly like successive incremental updates would (CRDT merge is
/// order- and redundancy-tolerant by construction — see <c>YcsPinTests.ReApplyingTheSameUpdateChangesNothing</c>),
/// and it is simpler to construct than tracking per-merge state vectors.</para>
/// </summary>
public sealed class CrdtDocGrainClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<CrdtTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<CrdtTestClientConfigurator>();
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
    // 0 — a STOPPED document must not answer like a successful replay.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MergingIntoAStoppedDocumentSaysSoInsteadOfAnsweringLikeAnIdempotentReplay()
    {
        // Found on review of this wave: the grain's defensive floor returned a bare CrdtMergeResult, so
        // "0 applied, 0 rows" came back — byte-identical to the successful idempotent replay the very
        // next test pins. An edge draining its store-and-forward buffer into a stopped document would
        // have read its own data loss as success. The diagnostic is the whole fix, so the diagnostic is
        // what this asserts; the zeroes were never the problem.
        var name = "crdt_stopped_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);

        await grain.StartAsync(MakeCrdtSource(name));
        await grain.StopAsync();

        var doc = new YDoc();
        doc.Transact(_ =>
        {
            var entity = new YMap();
            Root(doc).Set("e1", entity);
            entity.Set("name", "ignored");
        });

        var result = await grain.MergeAsync([doc.EncodeStateAsUpdateV1()]);

        Assert.Equal(0, result.UpdatesApplied);
        Assert.Equal(0, result.RowsEmitted);
        Assert.Contains(result.Diagnostics, d => d.Contains("not running", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // 1 — D7: re-merging the identical batch emits zero rows. The single most important test in this
    // wave — the property the entire store-and-forward design rests on.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ReMergingTheIdenticalBatchEmitsZeroRows()
    {
        var name = "crdt_idem_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update = edge.EncodeStateAsUpdateV1();

            var first = await grain.MergeAsync([update]);
            Assert.Equal(1, first.UpdatesApplied);
            Assert.Equal(1, first.RowsEmitted);

            var replay = await grain.MergeAsync([update]);
            Assert.Equal(1, replay.UpdatesApplied); // the update itself still decodes and applies fine
            Assert.Equal(0, replay.RowsEmitted);     // but changes nothing, so nothing is emitted

            // A THIRD replay, for good measure — the property has to hold indefinitely, not just once.
            var replayAgain = await grain.MergeAsync([update]);
            Assert.Equal(0, replayAgain.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 2 — a document edit reaches a table exactly like any other source's rows.
    // ------------------------------------------------------------------

    [Fact]
    public async Task AMergedEditEmitsARowStampedWithSourceAndTs()
    {
        var name = "crdt_emit_" + Guid.NewGuid().ToString("n")[..8];

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        var subHandle = await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var beforeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var result = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
            Assert.Equal(1, result.RowsEmitted);

            var row = await PollUntilAsync(
                () => Task.FromResult(Snapshot(received).FirstOrDefault(e => (string?)e.GetValueOrDefault("id") == "e1")),
                r => r is not null,
                deadlineSeconds: 15);

            Assert.NotNull(row);
            Assert.Equal(name, row!.Source);
            Assert.True(row.Timestamp >= beforeMs);
            Assert.Equal("Ann", row.GetValueOrDefault("name"));
            Assert.Equal("c", row.GetValueOrDefault("_op"));
            Assert.Equal(1L, Convert.ToInt64(row.GetValueOrDefault("_weight")));
        }
        finally
        {
            await subHandle.UnsubscribeAsync();
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 3 — deleting a key emits the tombstone convention (_op = "d", _weight = -1).
    // ------------------------------------------------------------------

    [Fact]
    public async Task DeletingAKeyEmitsTheTombstone()
    {
        var name = "crdt_delete_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var created = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);
            Assert.Equal(1, created.RowsEmitted);

            Root(edge).Delete("e1");
            var deleted = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);

            Assert.Equal(1, deleted.RowsEmitted);
            Assert.DoesNotContain(deleted.Diagnostics, d => d.Contains("failed", StringComparison.OrdinalIgnoreCase));

            var status = await grain.GetStatusAsync();
            Assert.Equal(0, status.EntityCount); // the key no longer enumerates at all
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 4 — a corrupt update in the middle of a batch: the good ones on either side still merge.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ACorruptUpdateInTheMiddleOfABatchDoesNotStrandTheGoodOnes()
    {
        var name = "crdt_corrupt_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update1 = edge.EncodeStateAsUpdateV1();

            var e2 = new YMap();
            Root(edge).Set("e2", e2);
            e2.Set("name", "Bo");
            var update2 = edge.EncodeStateAsUpdateV1(); // full state: e1 (unchanged) + e2 (new)

            // Not a legal v1 update frame — Ycs's decoder is expected to throw decoding it.
            var corrupt = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

            var result = await grain.MergeAsync([update1, corrupt, update2]);

            Assert.Equal(2, result.UpdatesApplied); // update1 and update2 — corrupt is NOT counted
            Assert.Equal(2, result.RowsEmitted);     // e1 created, e2 created
            Assert.Contains(result.Diagnostics, d => d.Contains("update[1]") && d.Contains("skipped"));

            var status = await grain.GetStatusAsync();
            Assert.Equal(2, status.EntityCount);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 5 — self-resume: a real deactivation, and a resume from durable storage.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ADeactivatedDocumentResumesFromPersistedStateAndItsContentSurvives()
    {
        var name = "crdt_resume_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update = edge.EncodeStateAsUpdateV1();

            var merged = await grain.MergeAsync([update]);
            Assert.Equal(1, merged.RowsEmitted);

            // Deactivate for real (the same mechanism ConnectorGrainPolledClusterTests uses) — this is
            // what proves OnActivateAsync's resume path, not just a warm in-memory field surviving.
            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            // Wake it with an ordinary grain call. GetStatusAsync must reflect the SAME document content
            // — 1 entity — read back out of grain storage by the new activation, not an empty fresh doc.
            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);
            Assert.Equal(1, status.UpdatesMerged);
            Assert.Equal(1, status.RowsEmitted);

            // D7 again, now against the RESUMED activation specifically: replaying the same update must
            // still emit nothing, which is only true if the resumed _doc genuinely has e1 already in it
            // (a fresh empty document would instead emit a create row here).
            var replay = await grain.MergeAsync([update]);
            Assert.Equal(0, replay.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 6 — Plan 020 wave D, finding 3: MergeAttributedAsync is functionally MergeAsync (D7, row shape,
    // diagnostics) plus attribution — never a second algorithm. GetUserByClientId/GetUserByDeletedId
    // themselves are pinned standalone in CrdtAttributionTests (Connectors.Crdt.Tests), which is where
    // the actual PermanentUserData mechanism is provable without a socket; there is no grain accessor
    // for "users" map content by design (plan 020's own cut list: no raw-document inspector), so what
    // this file proves is the WIRING — attribution never leaks into the projection, and never breaks
    // idempotence, including across a real restart (so the attribution bytes AttributeAcceptedUpdates
    // appends to PendingUpdates genuinely round-trip through RehydrateDoc like any other accepted byte).
    // ------------------------------------------------------------------

    private static SourceDefinition MakeAttributedCrdtSource(string name) => new()
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
            Crdt = new CrdtSourceConfig { RootMap = "root", KeyField = "id", AttributeChanges = true },
        },
    };

    [Fact]
    public async Task AttributedMergeEmitsExactlyTheSameRowAsAnOrdinaryMergeNothingFromTheUsersMapLeaksIn()
    {
        var name = "crdt_attr_emit_" + Guid.NewGuid().ToString("n")[..8];

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
        var received = new List<EventRecord>();
        var subHandle = await stream.SubscribeAsync((evt, _) =>
        {
            lock (received) received.Add(evt);
            return Task.CompletedTask;
        });

        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeAttributedCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var result = await grain.MergeAttributedAsync([edge.EncodeStateAsUpdateV1()], "alice");
            Assert.Equal(1, result.UpdatesApplied);
            Assert.Equal(1, result.RowsEmitted); // exactly one row — attribution's own "users" map writes never project

            var row = await PollUntilAsync(
                () => Task.FromResult(Snapshot(received).FirstOrDefault(e => (string?)e.GetValueOrDefault("id") == "e1")),
                r => r is not null,
                deadlineSeconds: 15);
            Assert.NotNull(row);
            Assert.Equal("Ann", row!.GetValueOrDefault("name"));

            // Give the stream a moment past the one expected row, then assert nothing ELSE ever arrived
            // (a "users" map row would be the failure mode this test exists to catch).
            await Task.Delay(200);
            Assert.Single(Snapshot(received));
        }
        finally
        {
            await subHandle.UnsubscribeAsync();
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task AttributedMergePreservesD7IdempotenceAcrossARealRestart()
    {
        var name = "crdt_attr_resume_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeAttributedCrdtSource(name));

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");
            var update = edge.EncodeStateAsUpdateV1();

            var first = await grain.MergeAttributedAsync([update], "alice");
            Assert.Equal(1, first.RowsEmitted);

            // A second attributed merge by the SAME actor: SetUserMapping must not be called again
            // (CrdtDocGrain.AttributeAcceptedUpdates's own GetUserByClientId short-circuit), and — the
            // property this test actually exists to check — D7 must still hold with attribution turned
            // on: re-delivering the identical update emits nothing.
            var replay = await grain.MergeAttributedAsync([update], "alice");
            Assert.Equal(0, replay.RowsEmitted);

            // A real restart. If AttributeAcceptedUpdates's produced bytes were NOT captured into
            // PendingUpdates (the load-bearing append documented at its call site), this would still
            // pass today (the entity's own content survives regardless) but would be silently losing
            // attribution on every compaction/restart — this at least proves the restart itself does not
            // throw or corrupt the document with the attribution machinery wired in, and that D7 holds
            // for the RESUMED activation exactly as CrdtDocGrainClusterTests' own un-attributed version
            // (test 5 above) already established for the plain path.
            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            var status = await grain.GetStatusAsync();
            Assert.Equal(1, status.EntityCount);

            var replayAfterRestart = await grain.MergeAttributedAsync([update], "alice");
            Assert.Equal(0, replayAfterRestart.RowsEmitted);
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task PlainMergeAsyncIgnoresAttributeChangesWhenNoActorIsSupplied()
    {
        // MergeAsync delegates to MergeCoreAsync(updates, actor: null) — actor is null regardless of the
        // source's own AttributeChanges config, so a caller of the UNATTRIBUTED method never triggers
        // PermanentUserData at all, even on a source that opted in. This is what keeps MergeAsync's own
        // signature and behaviour frozen (CrdtDocGrainClusterTests' first five tests all call it and must
        // keep passing unmodified).
        var name = "crdt_attr_unused_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeAttributedCrdtSource(name)); // AttributeChanges = true on the source

            var edge = new YDoc();
            var e1 = new YMap();
            Root(edge).Set("e1", e1);
            e1.Set("name", "Ann");

            var result = await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]); // the plain, un-attributed call
            Assert.Equal(1, result.RowsEmitted);
            Assert.Empty(result.Diagnostics); // no attribution-related diagnostic, because attribution never ran
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    // ------------------------------------------------------------------
    // 7 — Plan 020 wave F: escrow rebalance, against a real grain. The concurrent-overspend proof
    // itself (several YDocs that never see each other, spending past their own allowance in mutual
    // ignorance, merged only afterward) is EscrowCounterTests' job in Connectors.Crdt.Tests — that is
    // where "no synchronous coordination" is actually exercised, since a single grain here is one
    // coordinator by construction. What THIS file proves is the WIRING: RebalanceAsync reaches
    // EscrowCounter.TryTransfer correctly, GetStatusAsync's Escrow field reflects it (limit 2's
    // "visible, not silent"), a refusal writes nothing, and the transfer survives a real restart —
    // exactly the same restart proof test 5 above already gives the plain merge path.
    // ------------------------------------------------------------------

    private static SourceDefinition MakeEscrowCrdtSource(string name) => new()
    {
        Name = name,
        Kind = SourceKinds.Crdt,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("name", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Crdt = new CrdtSourceConfig
            {
                RootMap = "root",
                KeyField = "id",
                Escrow = new CrdtEscrowConfig
                {
                    CounterMap = "escrow",
                    // DECLARED BEHAVIOUR CHANGE, made during review of this same (unlanded) wave: the
                    // rebalance RPC originally moved allowance between two SPENDING replicas, and that was
                    // shown live to breach the bound — 16 spent against a bound of 10, with every caller
                    // behaving correctly (EscrowCounterTests' own
                    // ACoordinatorMayNotTransferOutOfASpendingReplica_TheSequenceThatBreachedTheBound
                    // pins the sequence). A coordinator may only give from the non-spending reserve, so
                    // these tests declare one; replica-to-replica transfers are the giver's own to make.
                    ReserveReplica = "reserve",
                    InitialAllowance = new Dictionary<string, long>
                    {
                        ["reserve"] = 3, ["site-a"] = 3, ["site-b"] = 3,
                    },
                },
            },
        },
    };

    [Fact]
    public async Task RebalanceMovesAllowanceAndStatusReflectsItIncludingTheExhaustedFlag()
    {
        var name = "crdt_escrow_reb_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeEscrowCrdtSource(name));

            // site-a spends its whole share via an ordinary content edit carrying no escrow keys at
            // all — this file's OTHER tests already prove content merges; the point here is only that
            // site-a is now exhausted (0 of 3 remaining).
            var edge = new YDoc();
            var counter = edge.GetMap("escrow");
            counter.Set("d:site-a", 3L);
            await grain.MergeAsync([edge.EncodeStateAsUpdateV1()]);

            var beforeStatus = await grain.GetStatusAsync();
            Assert.NotNull(beforeStatus.Escrow);
            var siteA = beforeStatus.Escrow!.Replicas.Single(r => r.Replica == "site-a");
            Assert.Equal(0, siteA.LocalAllowance);
            Assert.True(siteA.Exhausted);

            // Rebalance: the reserve (which never spends, and is therefore the only replica a
            // coordinator may safely give from) transfers 2 to the exhausted site-a.
            var rebalance = await grain.RebalanceAsync("reserve", "site-a", 2);
            Assert.True(rebalance.Ok);
            Assert.Equal(1, rebalance.FromAllowance); // reserve: 3 - 2
            Assert.Equal(2, rebalance.ToAllowance);   // site-a: 0 + 2

            var afterStatus = await grain.GetStatusAsync();
            var siteAAfter = afterStatus.Escrow!.Replicas.Single(r => r.Replica == "site-a");
            Assert.Equal(2, siteAAfter.LocalAllowance);
            Assert.False(siteAAfter.Exhausted); // reconnecting/rebalancing is what un-sticks it

            // The global bound is untouched by the transfer — it only ever moves allowance around.
            Assert.Equal(9, afterStatus.Escrow.Bound); // reserve 3 + site-a 3 + site-b 3
            Assert.Equal(3, afterStatus.Escrow.TotalSpent); // only site-a's original spend
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task RebalanceRefusesToTransferMoreThanTheSenderCurrentlyHoldsAndWritesNothing()
    {
        var name = "crdt_escrow_over_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeEscrowCrdtSource(name));

            var before = await grain.GetStatusAsync();

            var rebalance = await grain.RebalanceAsync("reserve", "site-b", 999); // only 3 in the reserve
            Assert.False(rebalance.Ok);
            Assert.Contains("holds only 3", rebalance.Reason);

            var after = await grain.GetStatusAsync();
            // Nothing moved — same allowances before and after the refused call.
            Assert.Equal(
                before.Escrow!.Replicas.Select(r => r.LocalAllowance).ToArray(),
                after.Escrow!.Replicas.Select(r => r.LocalAllowance).ToArray());
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task RebalanceOnASourceWithNoEscrowConfiguredIsRefusedNotSilent()
    {
        var name = "crdt_escrow_none_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeCrdtSource(name)); // no Escrow set

            var result = await grain.RebalanceAsync("a", "b", 1);
            Assert.False(result.Ok);
            Assert.Contains("no escrow counter configured", result.Reason);

            var status = await grain.GetStatusAsync();
            Assert.Null(status.Escrow); // ordinary documents are unaffected by this wave
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    [Fact]
    public async Task RebalanceOnAStoppedDocumentIsRefusedNotAnEmptyIndistinguishableFromSuccess()
    {
        var name = "crdt_escrow_stopped_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);

        await grain.StartAsync(MakeEscrowCrdtSource(name));
        await grain.StopAsync();

        var result = await grain.RebalanceAsync("site-a", "site-b", 1);
        Assert.False(result.Ok);
        Assert.Contains("not running", result.Reason);
    }

    [Fact]
    public async Task ARebalanceSurvivesARealRestartLikeAnyOtherAcceptedUpdate()
    {
        var name = "crdt_escrow_resume_" + Guid.NewGuid().ToString("n")[..8];
        var grain = _cluster.GrainFactory.GetGrain<ICrdtDocGrain>(name);
        try
        {
            await grain.StartAsync(MakeEscrowCrdtSource(name));

            var rebalance = await grain.RebalanceAsync("reserve", "site-a", 2);
            Assert.True(rebalance.Ok);

            // Deactivate for real — the same mechanism test 5 above uses to prove OnActivateAsync's
            // resume path, not just a warm in-memory field surviving.
            await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

            var status = await grain.GetStatusAsync();
            var siteA = status.Escrow!.Replicas.Single(r => r.Replica == "site-a");
            var reserve = status.Escrow.Replicas.Single(r => r.Replica == "reserve");
            Assert.Equal(5, siteA.LocalAllowance);  // 3 initial + 2 received
            Assert.Equal(1, reserve.LocalAllowance); // 3 initial - 2 given away
        }
        finally
        {
            await grain.StopAsync();
        }
    }

    private static List<EventRecord> Snapshot(List<EventRecord> list)
    {
        lock (list) return [.. list];
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last))
            {
                return last;
            }
            await Task.Delay(100);
        }
        return last;
    }
}
