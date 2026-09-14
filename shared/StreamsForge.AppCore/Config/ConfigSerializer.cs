using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Config;

/// <summary>
/// Plan 006 (D-I): parses config documents (JSON or YAML) into <see cref="ConfigDocument"/>,
/// renders the canonical byte-stable JSON (and an equivalent-content YAML), and maps a running
/// catalog into a document for export. See <see cref="ConfigJsonMapper"/> for exactly how the
/// canonical shape and omission rules are defined — this class is the thin public façade over it.
/// </summary>
public static class ConfigSerializer
{
    /// <summary>Parses JSON or YAML (sniffed by the first non-whitespace character — <c>{</c> is
    /// JSON, anything else is YAML) into a <see cref="ConfigDocument"/>. Returns (null,
    /// diagnostics) for unparseable text, an unsupported/non-integer <c>version</c>, or a
    /// structurally wrong root shape; returns a non-null document (with diagnostics describing what
    /// was dropped) when only individual entities were malformed — e.g. an entity missing "name" is
    /// skipped rather than failing the whole parse.</summary>
    public static (ConfigDocument? Doc, IReadOnlyList<string> Diagnostics) Parse(string text)
    {
        var (node, diagnostics) = ConfigJsonMapper.TextToNode(text);
        if (node is null)
        {
            return (null, diagnostics);
        }

        var (doc, docDiagnostics) = ConfigJsonMapper.NodeToDocument(node);
        diagnostics.AddRange(docDiagnostics);
        return (doc, diagnostics);
    }

    /// <summary>The canonical, byte-stable JSON rendering (D-I): 2-space indent, camelCase,
    /// entities sorted by Name (ordinal), properties in fixed declaration order, empties/nulls
    /// omitted per <see cref="ConfigJsonMapper"/>'s documented rule. <c>Parse(ToCanonicalJson(doc))</c>
    /// round-trips to an equal document, and <c>ToCanonicalJson</c> is idempotent under that
    /// round-trip (serialize → parse → serialize produces an identical string) — pinned by
    /// ConfigSerializerTests.</summary>
    public static string ToCanonicalJson(ConfigDocument doc) =>
        ConfigJsonMapper.ToCanonicalJsonText(ConfigJsonMapper.DocumentToNode(doc));

    /// <summary>Same content and ordering as <see cref="ToCanonicalJson"/>, rendered as YAML
    /// (2-space indent, block sequences). No byte-equality contract for YAML — only JSON has one
    /// (D-I) — but it is deterministic for a given document and round-trips through
    /// <see cref="Parse"/> correctly (string scalars are always quoted so a value that merely looks
    /// like a bool/number/null round-trips as a string).</summary>
    public static string ToYaml(ConfigDocument doc) =>
        ConfigJsonMapper.NodeToYaml(ConfigJsonMapper.DocumentToNode(doc));

    /// <summary>Maps a running catalog into a config document. <see cref="SourceDefinition"/> is
    /// copied whole (sources are already id-less — nothing to strip); pipelines/tables are mapped
    /// to <see cref="ConfigPipeline"/>/<see cref="ConfigTable"/> with <c>Running = Status !=
    /// PipelineStatus.Stopped</c> (so Failed exports as Running == true — see
    /// <see cref="ConfigPipeline.Running"/>'s doc comment). When <paramref name="includeSecrets"/>
    /// is false (the default posture — D-H), every source is passed through
    /// <see cref="SecretsMasker.Mask"/> first so header values / gRPC password+token never leave
    /// the process in a plain export; plan 009 B2 extends the identical rule to Sinks' NatsPubConfig
    /// credentials via <see cref="ToConfigPipeline"/>/<see cref="ToConfigTable"/>'s own
    /// <paramref name="includeSecrets"/> parameter.</summary>
    public static ConfigDocument FromCatalog(
        IReadOnlyList<SourceDefinition> sources,
        IReadOnlyList<PipelineDefinition> pipelines,
        IReadOnlyList<TableDefinition> tables,
        bool includeSecrets) => new()
    {
        Sources = [.. sources.Select(s => includeSecrets ? ConfigJsonMapper.DeepCloneModel(s) : SecretsMasker.Mask(s))],
        Pipelines = [.. pipelines.Select(p => ToConfigPipeline(p, includeSecrets))],
        Tables = [.. tables.Select(t => ToConfigTable(t, includeSecrets))],
    };

    /// <summary>Catalog PipelineDefinition -&gt; ConfigPipeline mapping (D-I "Running = Status !=
    /// Stopped"). Internal: shared by <see cref="FromCatalog"/> and <see cref="ImportPlanner"/>
    /// (which needs the same mapping to compare a doc entity against the stored catalog entity on
    /// equal footing — always called with the default <paramref name="includeSecrets"/> = true there,
    /// since ImportPlanner compares real Contracts objects, not an exported document; masking only
    /// matters at the <see cref="FromCatalog"/> export boundary).
    ///
    /// <para><b>Plan 016 wave 3-B: carries <see cref="PipelineDefinition.DependsOn"/>.</b> This feeds
    /// <c>CatalogRevisions.PipelineCanonicalText</c> (this exact projection is what that method
    /// canonicalizes), which is the predicate <see cref="ImportPlanner"/> uses for "skipped" vs
    /// "updated" AND the one that decides whether the registry bumps <c>Revision</c>. So as of this
    /// change, editing a pin — adding, removing, or changing an <c>EntityPin</c>'s
    /// <c>schemaRevision</c> — is a DEFINITION change that bumps <c>Revision</c>. Deliberate: a pin is
    /// part of what the entity IS, the same way its SQL or its sinks are. Verified not to disturb the
    /// "an identical round-trip plans as skipped" property, because an unchanged pin list still
    /// canonicalizes identically.</para>
    ///
    /// <para><b>Plan 026 wave 3: also carries <see cref="PipelineDefinition.ReplayFrom"/>/
    /// <see cref="PipelineDefinition.Replay"/>.</b> Wave 2 left these out of the projection, so a
    /// <c>replayFrom</c>-only edit restarted the pipeline (the grain applies it directly) without ever
    /// bumping <c>Revision</c> — found and documented, not fixed, in plan 026's wave 2 outcomes. Adding
    /// them here closes that gap the same way DependsOn closed it for pins.</para></summary>
    internal static ConfigPipeline ToConfigPipeline(PipelineDefinition p, bool includeSecrets = true) => new()
    {
        Name = p.Name,
        Description = p.Description,
        Sql = p.Sql,
        Running = p.Status != PipelineStatus.Stopped,
        Tags = [.. p.Tags],
        Metadata = new Dictionary<string, string>(p.Metadata),
        Sinks = includeSecrets ? [.. p.Sinks] : SecretsMasker.MaskSinks(p.Sinks),
        DependsOn = [.. p.DependsOn],
        ReplayFrom = new Dictionary<string, ReplayFrom>(p.ReplayFrom),
        Replay = p.Replay,
    };

    /// <summary>Catalog TableDefinition -&gt; ConfigTable mapping — same Running rule as
    /// <see cref="ToConfigPipeline"/> plus the table-only knobs, runtime/derived fields (id, Status,
    /// Error, CreatedBy/timestamps, OutputFields, StreamInputs/TableInputs) dropped.
    ///
    /// <para>Plan 016 wave 3-B: carries <see cref="TableDefinition.DependsOn"/> — see the identical
    /// Revision-bump note on <see cref="ToConfigPipeline"/>, which applies here via
    /// <c>CatalogRevisions.TableCanonicalText</c> the same way.</para>
    ///
    /// <para>Plan 026 wave 3: also carries <see cref="TableDefinition.ReplayFrom"/>/
    /// <see cref="TableDefinition.Replay"/> — see the identical note on <see cref="ToConfigPipeline"/>.
    /// <c>SchemaRevision</c> is untouched either way: it moves only on an <c>OutputFields</c> shape
    /// change (<c>CatalogRevisions.BumpTable</c>), and replay is not shape.</para></summary>
    internal static ConfigTable ToConfigTable(TableDefinition t, bool includeSecrets = true) => new()
    {
        Name = t.Name,
        Description = t.Description,
        Sql = t.Sql,
        Running = t.Status != PipelineStatus.Stopped,
        Tags = [.. t.Tags],
        Metadata = new Dictionary<string, string>(t.Metadata),
        SearchEnabled = t.SearchEnabled,
        SearchMode = t.SearchMode.ToString(),
        HistoryEnabled = t.HistoryEnabled,
        HistoryMode = t.HistoryMode.ToString(),
        HistoryLimit = t.HistoryLimit,
        HistoryByField = t.HistoryByField,
        HistoryWindowMs = t.HistoryWindowMs,
        RetentionMaxRows = t.RetentionMaxRows,
        RetentionTtlMs = t.RetentionTtlMs,
        ShardBy = [.. t.ShardBy],
        Parallelism = t.Parallelism,
        Sinks = includeSecrets ? [.. t.Sinks] : SecretsMasker.MaskSinks(t.Sinks),
        DependsOn = [.. t.DependsOn],
        ReplayFrom = new Dictionary<string, ReplayFrom>(t.ReplayFrom),
        Replay = t.Replay,
    };
}
