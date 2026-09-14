using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 026 wave 2, D5 — <c>replayFrom</c> on a TABLE/PIPELINE definition: a consumer created after its
/// input already produced starts from a POSITION, and it does so only ever against a FRESH executor
/// (which is what keeps replayed rows — carrying old <c>_ts</c> values — out of <c>LateEvents</c>).
///
/// <para>Where wave 1's <see cref="SourceReplayFromClusterTests"/> drove the producer gate directly, this
/// suite drives the CONSUMER side: <c>TableGrain.AttachToStreamInputAsync</c> /
/// <c>SubscribeToPipelineInputAsync</c>, <c>PipelineGrain.AttachToSourceAsync</c> and (Parallelism == 2)
/// <c>TableIngestGrain</c>, plus <c>RegistryGrain</c>'s validation and the restart-on-change rule.</para>
///
/// <para><b>Why the counts are exact.</b> The file sources carry a dedup key, so their rows are emitted
/// once; the tables are <c>LATEST BY (id)</c> over distinct ids, so a re-delivery would update a key
/// rather than add one; and every test quiesces 2 s after the last publish before attaching, per plan
/// 023's "ONE GAP, MEASURED" paragraph (the attach hold stops PUBLISHING, not the stream provider's own
/// pull-into-cache pipeline, so attaching inside ~one pull period of the last publish can see a row both
/// live and replayed).</para>
///
/// <para>Own single-silo <see cref="TestCluster"/> reusing the <c>internal</c>
/// <see cref="ConnectorTestSiloConfigurator"/>/<see cref="ConnectorTestClientConfigurator"/> — the same
/// cross-file reuse <see cref="TableOverPipelineClusterTests"/> already does. <see cref="PollUntilAsync"/>
/// is a copy of the identically-named private helper there, per this repo's one-owner-per-file rule.</para>
/// </summary>
public sealed class ReplayFromClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-replayfrom-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_scratchDir, recursive: true); } catch { /* best-effort */ }
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        if (until(last)) return last;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }

    private static string Suffix() => Guid.NewGuid().ToString("n")[..8];

    /// <summary>ids 1..count, so a 1-based producer POSITION and the row's own id are the same number —
    /// which is what makes "from seq 401" assertable as "ids 401..500" rather than as an off-by-one.</summary>
    private static string Ndjson(int count) =>
        string.Concat(Enumerable.Range(1, count).Select(i => $"{{\"id\":{i},\"value\":\"v{i}\"}}\n"));

    private static SourceDefinition FileSource(string name, string path) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.Long), new FieldDef("value", FieldType.String)],
        Connector = new ConnectorConfig
        {
            Schedule = new ScheduleSpec { IntervalMs = 1000 }, // the floor — ScheduleCalc.MinIntervalMs
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                DedupKeyField = "id",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
                    new FieldMapEntry { Field = new FieldDef("value", FieldType.String) },
                ],
            },
        },
    };

    /// <summary>Upserts a file source holding <paramref name="rows"/> rows, waits for it to emit all of
    /// them, then quiesces — so everything after this returns is a genuinely LATE consumer.</summary>
    private async Task<string> DrainedFileSourceAsync(string name, int rows)
    {
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(rows));
        await Registry.UpsertSourceAsync(FileSource(name, path));

        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var status = await PollUntilAsync(() => connector.GetStatusAsync(), s => s.EventsEmittedTotal >= rows, deadlineSeconds: 90);
        Assert.Equal(rows, status.EventsEmittedTotal);
        await Task.Delay(2000);
        return name;
    }

    private async Task<TableDefinition> StartTableAsync(
        string name, string sql, Dictionary<string, ReplayFrom>? replayFrom = null, int parallelism = 1)
    {
        var table = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = name,
            Sql = sql,
            Parallelism = parallelism,
            ReplayFrom = replayFrom ?? [],
        });
        await Registry.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        return table;
    }

    private static List<long> IdsOf(IEnumerable<TableRowDto> rows) =>
        rows.Select(r => Convert.ToInt64(r.Row["id"])).OrderBy(x => x).ToList();

    // ================================================================================================
    // (1) a position never returns LESS than plan 023's default attach — the regression guard
    // ================================================================================================

    [Fact]
    public async Task Table_with_replayFrom_seq_1_over_an_already_polled_source_gets_all_500()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfsrc_" + s, 500);

        var all = await StartTableAsync(
            "rfall_" + s,
            $"SELECT id, value FROM {source} LATEST BY (id)",
            new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 1 } });

        var allGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(all.Name);
        await PollUntilAsync(() => allGrain.GetRowCountAsync(), c => c >= 500, deadlineSeconds: 90);
        await Task.Delay(1500);
        Assert.Equal(500, await allGrain.GetRowCountAsync());

        var tail = await StartTableAsync(
            "rftail_" + s,
            $"SELECT id, value FROM {source} LATEST BY (id)",
            new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 401 } });

        var tailGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tail.Name);
        await PollUntilAsync(() => tailGrain.GetRowCountAsync(), c => c >= 100, deadlineSeconds: 90);
        await Task.Delay(1500);
        Assert.Equal(100, await tailGrain.GetRowCountAsync());
        Assert.Equal(
            Enumerable.Range(401, 100).Select(i => (long)i).ToList(),
            IdsOf(await tailGrain.GetRowsAsync(2000, 0)));
    }

    /// <summary>The Parallelism == 2 twin of the fact above — same source, same position, but the attach
    /// runs in <c>TableIngestGrain</c> rather than in <c>TableGrain</c>'s classic path.</summary>
    [Fact]
    public async Task Partitioned_table_with_replayFrom_replays_through_its_ingest_grain()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfpsrc_" + s, 500);

        var table = await StartTableAsync(
            "rfptbl_" + s,
            $"SELECT id, value FROM {source} LATEST BY (id)",
            new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 401 } },
            parallelism: 2);
        Assert.Equal(2, table.Parallelism);

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c >= 100, deadlineSeconds: 90);
        await Task.Delay(1500);
        Assert.Equal(100, await grain.GetRowCountAsync());
    }

    // ================================================================================================
    // (2) a GENERATOR — the kind the default attach deliberately leaves live-only
    // ================================================================================================

    [Fact]
    public async Task Table_with_replayFrom_over_a_generator_gets_rows_from_that_position()
    {
        var s = Suffix();
        var name = "rfgen_" + s;
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = SourceKinds.Generator,
            GeneratorProfile = "trades",
            Enabled = true,
            EventsPerSecond = 100,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
                new FieldDef("side", FieldType.String),
                new FieldDef("venue", FieldType.String),
            ],
        });

        var generator = _cluster.GrainFactory.GetGrain<IGeneratorGrain>(name);

        // 300 events at 100/s is ~3 s of timer ticks idle; 90 s for the same CPU-contention reason wave 1's
        // generator fact carries (it reached only 238/300 in 30 s under a whole-solution run).
        long lastSeq = 0;
        var deadline = DateTime.UtcNow.AddSeconds(90);
        while (DateTime.UtcNow < deadline && lastSeq < 300)
        {
            try { lastSeq = (await generator.BeginAttachAsync(null)).LastSeq; }
            finally { await generator.EndAttachAsync(); }
            if (lastSeq < 300) await Task.Delay(100);
        }
        Assert.True(lastSeq >= 300, $"generator only reached seq {lastSeq} within the deadline");

        // Stop the timer before pinning the boundary — otherwise a tick landing between reading LastSeq
        // and the table's own attach would make "exactly 50" a race rather than a fact. With the generator
        // quiet, the no-replayFrom table below is a genuine control: it can only be fed by replay.
        await generator.StopAsync();
        // Plan 023's quiescence window, and here it is load-bearing rather than belt-and-braces: the
        // tables below count with COUNT(*), so a row still sitting in the memory stream's queue when they
        // subscribe would be delivered live AND appear in the replayed snapshot, and "exactly 50" would
        // read 53 (observed under a whole-solution parallel run before this wait existed).
        await Task.Delay(2000);
        try { lastSeq = (await generator.BeginAttachAsync(null)).LastSeq; }
        finally { await generator.EndAttachAsync(); }

        // "trades" has no per-event unique field, so the counter IS the assertion: a table-mode running
        // COUNT(*) per symbol, summed, is exactly how many events the table admitted.
        var sql = $"SELECT symbol, COUNT(*) AS n FROM {name} GROUP BY symbol";
        var replayed = await StartTableAsync("rfgentbl_" + s, sql,
            new Dictionary<string, ReplayFrom> { [name] = new() { Seq = lastSeq - 49 } });
        var control = await StartTableAsync("rfgenctl_" + s, sql);

        var replayedGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(replayed.Name);
        var controlGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(control.Name);

        static long Total(List<TableRowDto> rows) => rows.Sum(r => Convert.ToInt64(r.Row["n"]));

        await PollUntilAsync(() => replayedGrain.GetRowsAsync(2000, 0), rows => Total(rows) >= 50, deadlineSeconds: 90);
        await Task.Delay(1500);
        Assert.Equal(50, Total(await replayedGrain.GetRowsAsync(2000, 0)));

        // The control table was created at the same moment over the same (stopped) generator and named no
        // position — so it sees nothing. That contrast is the decision this test exists to pin: an input
        // NOT named in replayFrom does NOT suddenly receive a generator's whole retained ring.
        Assert.Equal(0, Total(await controlGrain.GetRowsAsync(2000, 0)));
    }

    // ================================================================================================
    // (3) a table over a PIPELINE, from a result-batch position
    // ================================================================================================

    [Fact]
    public async Task Table_over_a_pipeline_with_replayFrom_gets_exactly_the_results_after_that_position()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfpipesrc_" + s, 300);

        // No window: PublishRowsAsync runs once per admitted event, so a BATCH position is the result
        // index, and (ids being 1..300) the id too.
        var pipeline = await Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "rfpipe_" + s,
            Sql = $"SELECT id, value FROM {source}",
        });
        await Registry.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);

        var pipeGrain = _cluster.GrainFactory.GetGrain<IPipelineGrain>(pipeline.Id);
        var metrics = await PollUntilAsync(() => pipeGrain.GetMetricsAsync(), m => m.TotalRowsOut >= 300, deadlineSeconds: 90);
        Assert.Equal(300, metrics.TotalRowsOut);
        await Task.Delay(2000);

        var table = await StartTableAsync(
            "rfpipetbl_" + s,
            $"SELECT id, value FROM {pipeline.Name} LATEST BY (id)",
            new Dictionary<string, ReplayFrom> { [pipeline.Name] = new() { Seq = 101 } });
        Assert.Equal([pipeline.Name], table.PipelineInputs.ToArray());

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c >= 200, deadlineSeconds: 90);
        await Task.Delay(1500);
        Assert.Equal(200, await grain.GetRowCountAsync());
        Assert.Equal(
            Enumerable.Range(101, 200).Select(i => (long)i).ToList(),
            IdsOf(await grain.GetRowsAsync(2000, 0)));
    }

    // ================================================================================================
    // (4) D5 itself: a WINDOWED pipeline replays without a single late event
    // ================================================================================================

    [Fact]
    public async Task Windowed_pipeline_with_replayFrom_replays_without_late_events()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfwinsrc_" + s, 500);

        var pipeline = await Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "rfwin_" + s,
            Sql = $"SELECT value, COUNT(*) AS n FROM {source} GROUP BY value WINDOW TUMBLING(SIZE 1 SECONDS)",
            ReplayFrom = new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 1 } },
        });
        Assert.Equal([source], pipeline.SourceNames.ToArray());

        await Registry.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);

        var grain = _cluster.GrainFactory.GetGrain<IPipelineGrain>(pipeline.Id);
        var metrics = await PollUntilAsync(() => grain.GetMetricsAsync(), m => m.TotalEventsIn >= 500, deadlineSeconds: 90);
        Assert.Equal(500, metrics.TotalEventsIn);

        // The whole of D5: the executor was brand new when the 500 rows (all carrying an already-past
        // `_ts`) were fed, and the wall-clock watermark tick is armed only AFTER that — so not one of them
        // was dropped as late. A replay into a LIVE executor would show 500 here instead of 0.
        await Task.Delay(2000);
        Assert.Equal(0, (await grain.GetMetricsAsync()).LateEvents);
    }

    // ================================================================================================
    // (5) changing replayFrom restarts a Running entity — it is executor-affecting, like the SQL
    // ================================================================================================

    [Fact]
    public async Task Changing_replayFrom_on_a_running_table_restarts_it()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfressrc_" + s, 500);

        var table = await StartTableAsync(
            "rfrestbl_" + s,
            $"SELECT id, value FROM {source} LATEST BY (id)",
            new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 401 } });

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c >= 100, deadlineSeconds: 90);
        Assert.Equal(100, await grain.GetRowCountAsync());

        var before = (await Registry.GetTableAsync(table.Id))!;
        Assert.Equal(PipelineStatus.Running, before.Status);

        // A fresh instance, never the stored one: mutating the record the registry already holds would
        // make `existing` and `def` the same object and the change undetectable.
        var updated = await Registry.UpdateTableAsync(new TableDefinition
        {
            Id = table.Id,
            Name = table.Name,
            Sql = table.Sql,
            ReplayFrom = new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 1 } },
        });
        Assert.NotNull(updated);
        Assert.Equal(PipelineStatus.Running, updated!.Status);

        // NOT asserted: a Revision bump. `CatalogRevisions.DefinitionChanged` compares the CONFIG
        // projection (`ConfigTable`), and `replayFrom` is not in it — the same documented ceiling
        // Persistence/FlushMs/JournalMaxEntries already sit on (see CatalogRevisions' own ponytail
        // paragraph). Widening that projection is a change to the config export contract and belongs with
        // wave 3, which owns the `replay` group's round-trip. The RESTART below is what D5 is actually
        // about, and it does not go through the revision counter at all.
        Assert.Equal(before.Revision, updated.Revision);

        // The restart is what is actually observable: a fresh executor replayed from position 1 instead
        // of 401, so the table now holds all 500 rows rather than the 100 it was built with.
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c >= 500, deadlineSeconds: 90);
        Assert.Equal(500, await grain.GetRowCountAsync());
    }

    [Fact]
    public async Task Changing_replayFrom_on_a_running_pipeline_restarts_it()
    {
        var s = Suffix();
        var source = await DrainedFileSourceAsync("rfrespsrc_" + s, 500);

        var pipeline = await Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "rfresp_" + s,
            Sql = $"SELECT id, value FROM {source}",
            ReplayFrom = new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 401 } },
        });
        await Registry.SetPipelineStatusAsync(pipeline.Id, PipelineStatus.Running);

        var grain = _cluster.GrainFactory.GetGrain<IPipelineGrain>(pipeline.Id);
        await PollUntilAsync(() => grain.GetMetricsAsync(), m => m.TotalEventsIn >= 100, deadlineSeconds: 90);
        Assert.Equal(100, (await grain.GetMetricsAsync()).TotalEventsIn);

        var before = (await Registry.GetPipelineAsync(pipeline.Id))!;

        var updated = await Registry.UpdatePipelineAsync(new PipelineDefinition
        {
            Id = pipeline.Id,
            Name = pipeline.Name,
            Sql = pipeline.Sql,
            ReplayFrom = new Dictionary<string, ReplayFrom> { [source] = new() { Seq = 1 } },
        });
        Assert.NotNull(updated);
        Assert.Equal(PipelineStatus.Running, updated!.Status);
        // Same config-projection ceiling as the table twin above — see its comment.
        Assert.Equal(before.Revision, updated.Revision);

        // The counters are cumulative across a restart (they live on the activation, not the executor),
        // so "restarted and replayed from 1" reads as 100 + 500.
        var after = await PollUntilAsync(() => grain.GetMetricsAsync(), m => m.TotalEventsIn >= 600, deadlineSeconds: 90);
        Assert.Equal(600, after.TotalEventsIn);
    }

    // ================================================================================================
    // (6) validation: the three refusals, with their reasons
    // ================================================================================================

    [Fact]
    public async Task ReplayFrom_naming_a_table_input_or_an_unknown_input_is_refused()
    {
        var s = Suffix();
        var path = Path.Combine(_scratchDir, "rfval_" + s + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(1));
        var source = "rfvalsrc_" + s;
        var disabled = FileSource(source, path);
        disabled.Enabled = false;
        await Registry.UpsertSourceAsync(disabled);

        var upstream = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = "rfvalup_" + s,
            Sql = $"SELECT id, value FROM {source} LATEST BY (id)",
        });

        // (a) a TABLE input — refused with the backfill-snapshot reason.
        var onTableInput = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.CreateTableAsync(new TableDefinition
        {
            Name = "rfvaldown_" + s,
            Sql = $"SELECT id, value FROM {upstream.Name} LATEST BY (id)",
            ReplayFrom = new Dictionary<string, ReplayFrom> { [upstream.Name] = new() { Seq = 1 } },
        }));
        Assert.Contains("a table input replays through the backfill snapshot", onTableInput.Message);

        // (b) a name that is no input of this table at all — refused NAMING the valid ones.
        var unknown = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.CreateTableAsync(new TableDefinition
        {
            Name = "rfvalunk_" + s,
            Sql = $"SELECT id, value FROM {source} LATEST BY (id)",
            ReplayFrom = new Dictionary<string, ReplayFrom> { ["not_an_input"] = new() { Seq = 1 } },
        }));
        Assert.Contains("which is not an input of this table", unknown.Message);
        Assert.Contains(source, unknown.Message);

        // (c) the same on UPDATE, not only on create — a Stopped table is edited into an illegal position.
        var ok = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = "rfvalok_" + s,
            Sql = $"SELECT id, value FROM {source} LATEST BY (id)",
        });
        var onUpdate = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdateTableAsync(new TableDefinition
        {
            Id = ok.Id,
            Name = ok.Name,
            Sql = ok.Sql,
            ReplayFrom = new Dictionary<string, ReplayFrom> { ["not_an_input"] = new() { Seq = 1 } },
        }));
        Assert.Contains("which is not an input of this table", onUpdate.Message);

        // (d) the pipeline twin of (b).
        var pipeUnknown = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "rfvalpipe_" + s,
            Sql = $"SELECT id, value FROM {source}",
            ReplayFrom = new Dictionary<string, ReplayFrom> { ["not_an_input"] = new() { Seq = 1 } },
        }));
        Assert.Contains("which is not an input of this pipeline", pipeUnknown.Message);
    }

    [Fact]
    public async Task ReplayFrom_naming_a_crdt_source_is_refused()
    {
        var s = Suffix();
        var name = "rfcrdt_" + s;
        // Disabled: an ENABLED crdt source needs the crdt plugin loaded (RegistryGrain refuses the upsert
        // by name otherwise), and no plugin is loaded in a bare TestCluster. Disabled is enough here — the
        // source still contributes its relation to the compile, which is all the validation reads.
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = name,
            Kind = SourceKinds.Crdt,
            Enabled = false,
            Fields = [new FieldDef("id", FieldType.Long), new FieldDef("value", FieldType.String)],
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.CreateTableAsync(new TableDefinition
        {
            Name = "rfcrdttbl_" + s,
            Sql = $"SELECT id, value FROM {name} LATEST BY (id)",
            ReplayFrom = new Dictionary<string, ReplayFrom> { [name] = new() { Seq = 1 } },
        }));
        Assert.Contains("crdt sources replay through their own ReplayAsync", ex.Message);
    }
}
