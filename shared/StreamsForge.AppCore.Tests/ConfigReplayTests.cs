using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 026 wave 3 — carrying <see cref="PipelineDefinition.ReplayFrom"/>/<see cref="PipelineDefinition.Replay"/>
/// (and the table equivalents) through <see cref="ConfigSerializer.FromCatalog"/>/<c>ToConfigPipeline</c>/
/// <c>ToConfigTable</c>, the JSON round-trip, and the consequence: because they feed
/// <c>CatalogRevisions.PipelineCanonicalText</c>/<c>TableCanonicalText</c>, a <c>replayFrom</c>/<c>replay</c>
/// edit is now a DEFINITION change — it plans as "updated" and bumps <c>Revision</c>, closing wave 2's
/// found-and-not-fixed gap #1 (see plans/026-stream-replay.md). Mirrors <c>ConfigDependsOnTests</c>' shape.
/// </summary>
public class ConfigReplayTests
{
    private static Dictionary<string, ReplayFrom> ReplayFromSeq(string input, long seq) =>
        new() { [input] = new ReplayFrom { Seq = seq } };

    // ------------------------------------------------------------------
    // Sources need NO mapping change: FromCatalog copies SourceDefinition whole (SourceNode
    // serializes the WHOLE model via reflection, unlike the trimmed ConfigPipeline/ConfigTable
    // projections), so a source's Replay group was already carried and already bumps its Revision
    // (CatalogRevisions.CarryAndBumpSource -> SourceCanonicalText -> the whole SourceNode) the moment
    // the pre-wave commit added the field to SourceDefinition. Pinned here rather than left as an
    // unverified claim.
    // ------------------------------------------------------------------

    [Fact]
    public void ASourceNeedsNoMappingChangeToExportReplay()
    {
        var source = new SourceDefinition { Name = "s", Replay = new ReplayConfig { Persist = true, RetainMs = 10 } };
        var doc = ConfigSerializer.FromCatalog([source], [], [], includeSecrets: true);

        Assert.True(doc.Sources[0].Replay!.Persist);
        Assert.Equal(10, doc.Sources[0].Replay!.RetainMs);
        Assert.Contains("\"replay\"", ConfigSerializer.ToCanonicalJson(doc));
    }

    [Fact]
    public void EditingASourcesReplayConfigPlansAsUpdatedAndBumpsRevision()
    {
        var stored = new SourceDefinition { Name = "s", Revision = 4, Replay = new ReplayConfig { Persist = false } };
        var incoming = new SourceDefinition { Name = "s", Replay = new ReplayConfig { Persist = true } };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(
            new ConfigDocument { Sources = [incoming] }, [stored], [], [], "merge")).Action);

        CatalogRevisions.CarryAndBumpSource(stored, incoming);
        Assert.Equal(5, incoming.Revision);
    }

    // ------------------------------------------------------------------
    // FromCatalog / ToConfigPipeline / ToConfigTable carry ReplayFrom/Replay.
    // ------------------------------------------------------------------

    [Fact]
    public void FromCatalogExportsPipelineReplayFromAndReplay()
    {
        var pipeline = new PipelineDefinition
        {
            Id = "p1",
            Name = "p",
            Sql = "SELECT 1",
            ReplayFrom = ReplayFromSeq("trades", 401),
            Replay = new ReplayConfig { Persist = true, RetainMs = 60_000, MaxBytes = 1024 },
        };

        var doc = ConfigSerializer.FromCatalog([], [pipeline], [], includeSecrets: true);

        var entry = Assert.Single(doc.Pipelines[0].ReplayFrom);
        Assert.Equal("trades", entry.Key);
        Assert.Equal(401, entry.Value.Seq);
        Assert.NotNull(doc.Pipelines[0].Replay);
        Assert.True(doc.Pipelines[0].Replay!.Persist);
        Assert.Equal(60_000, doc.Pipelines[0].Replay!.RetainMs);
        Assert.Equal(1024, doc.Pipelines[0].Replay!.MaxBytes);
    }

    [Fact]
    public void FromCatalogExportsTableReplayFromAndReplay()
    {
        var table = new TableDefinition
        {
            Id = "t1",
            Name = "t",
            Sql = "SELECT 1",
            ReplayFrom = new Dictionary<string, ReplayFrom> { ["src"] = new ReplayFrom { TimestampMs = 12345 } },
            Replay = new ReplayConfig { Persist = true },
        };

        var doc = ConfigSerializer.FromCatalog([], [], [table], includeSecrets: true);

        var entry = Assert.Single(doc.Tables[0].ReplayFrom);
        Assert.Equal("src", entry.Key);
        Assert.Equal(12345, entry.Value.TimestampMs);
        Assert.True(doc.Tables[0].Replay!.Persist);
    }

    [Fact]
    public void APipelineWithNoReplayConfigExportsNeitherField()
    {
        var pipeline = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1" };
        var doc = ConfigSerializer.FromCatalog([], [pipeline], [], includeSecrets: true);

        Assert.Empty(doc.Pipelines[0].ReplayFrom);
        Assert.Null(doc.Pipelines[0].Replay);
        var json = ConfigSerializer.ToCanonicalJson(doc);
        Assert.DoesNotContain("replayFrom", json);
        Assert.DoesNotContain("\"replay\"", json);
    }

    // ------------------------------------------------------------------
    // JSON round-trip.
    // ------------------------------------------------------------------

    [Fact]
    public void ReplayFromAndReplaySurviveTheCanonicalJsonRoundTrip()
    {
        var doc = new ConfigDocument
        {
            Pipelines =
            [
                new ConfigPipeline
                {
                    Name = "p",
                    Sql = "SELECT 1",
                    ReplayFrom = ReplayFromSeq("trades", 401),
                    Replay = new ReplayConfig { Persist = true, RetainMs = 5000, MaxBytes = 2048 },
                },
            ],
            Tables =
            [
                new ConfigTable
                {
                    Name = "t",
                    Sql = "SELECT 1",
                    ReplayFrom = new Dictionary<string, ReplayFrom> { ["u"] = new ReplayFrom { TimestampMs = 99 } },
                    Replay = new ReplayConfig { Persist = true },
                },
            ],
        };

        var json = ConfigSerializer.ToCanonicalJson(doc);
        var (parsed, diagnostics) = ConfigSerializer.Parse(json);

        Assert.Empty(diagnostics);
        Assert.NotNull(parsed);

        var pipelineEntry = Assert.Single(parsed!.Pipelines[0].ReplayFrom);
        Assert.Equal("trades", pipelineEntry.Key);
        Assert.Equal(401, pipelineEntry.Value.Seq);
        Assert.True(parsed.Pipelines[0].Replay!.Persist);
        Assert.Equal(5000, parsed.Pipelines[0].Replay!.RetainMs);
        Assert.Equal(2048, parsed.Pipelines[0].Replay!.MaxBytes);

        var tableEntry = Assert.Single(parsed.Tables[0].ReplayFrom);
        Assert.Equal("u", tableEntry.Key);
        Assert.Equal(99, tableEntry.Value.TimestampMs);
        Assert.True(parsed.Tables[0].Replay!.Persist);
    }

    [Fact]
    public void ADocumentWithNoReplayFromParsesToAnEmptyDictionary()
    {
        // NormalizeCollections' explicit-null -> [] rule, the same one DependsOn/Sinks/Tags already get.
        var (parsed, _) = ConfigSerializer.Parse("""{ "version": 1, "pipelines": [ { "name": "p", "sql": "SELECT 1", "running": false, "replayFrom": null } ] }""");
        Assert.NotNull(parsed);
        Assert.NotNull(parsed!.Pipelines[0].ReplayFrom);
        Assert.Empty(parsed.Pipelines[0].ReplayFrom);
    }

    // ------------------------------------------------------------------
    // The consequence: replayFrom/replay are part of the definition, so editing either bumps Revision.
    // ------------------------------------------------------------------

    [Fact]
    public void IdenticalReplayFromRoundTripsAsSkippedForAPipeline()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) };
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) }] };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void IdenticalReplayFromRoundTripsAsSkippedForATable()
    {
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) };
        var doc = new ConfigDocument { Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) }] };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    [Fact]
    public void EditingReplayFromPlansAsUpdatedForAPipeline()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) };
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 2) }] };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void EditingReplayPlansAsUpdatedForATable()
    {
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT 1", Replay = new ReplayConfig { Persist = false } };
        var doc = new ConfigDocument { Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1", Replay = new ReplayConfig { Persist = true } }] };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    /// <summary>The behaviour change stated as a fact rather than a claim, mirroring
    /// <c>ConfigDependsOnTests.APinEditThatPlansAsUpdatedAlsoBumpsRevision</c>: the SAME predicate
    /// ImportPlanner uses to say "updated" is what the registry bumps Revision on
    /// (CatalogRevisions.BumpPipeline/BumpTable) — this is exactly wave 2's found-and-not-fixed gap,
    /// closed.</summary>
    [Fact]
    public void AReplayFromEditThatPlansAsUpdatedAlsoBumpsPipelineRevision()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Revision = 4, ReplayFrom = ReplayFromSeq("trades", 1) };
        var incoming = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 2) };

        CatalogRecordMerge.CarryServerOwnedFields(stored, incoming, nowMs: 1);
        CatalogRevisions.BumpPipeline(stored, incoming);

        Assert.Equal(5, incoming.Revision);
    }

    [Fact]
    public void AReplayEditThatPlansAsUpdatedAlsoBumpsTableRevisionButNotSchemaRevision()
    {
        var stored = new TableDefinition
        {
            Id = "t1", Name = "t", Sql = "SELECT 1", Revision = 4, SchemaRevision = 2,
            Replay = new ReplayConfig { Persist = false },
        };
        var incoming = new TableDefinition
        {
            Id = "t1", Name = "t", Sql = "SELECT 1",
            Replay = new ReplayConfig { Persist = true },
        };

        CatalogRecordMerge.CarryServerOwnedFields(stored, incoming, nowMs: 1);
        CatalogRevisions.BumpTable(stored, incoming, previousOutputFields: stored.OutputFields);

        Assert.Equal(5, incoming.Revision);
        Assert.Equal(2, incoming.SchemaRevision); // replay is not shape — untouched.
    }

    [Fact]
    public void AnUnchangedReplayRoundTripDoesNotBumpRevision()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Revision = 4, ReplayFrom = ReplayFromSeq("trades", 1) };
        var incoming = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", ReplayFrom = ReplayFromSeq("trades", 1) };

        CatalogRecordMerge.CarryServerOwnedFields(stored, incoming, nowMs: 1);
        CatalogRevisions.BumpPipeline(stored, incoming);

        Assert.Equal(4, incoming.Revision);
    }
}
