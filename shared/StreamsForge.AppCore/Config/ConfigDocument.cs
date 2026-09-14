using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Config;

// Plan 006 (D-I/D-J): the config import/export document model. Deliberately NOT
// [GenerateSerializer] — these types never cross grain/actor transport, they live only at the
// config import/export API boundary (shared/StreamsForge.Api, a future wave) and inside this
// pure, runtime-free engine (parse/compose/plan/write). See ConfigSerializer for the JSON/YAML
// wire shape and ConfigComposer/ImportPlanner for how documents combine and diff against a
// running catalog.

/// <summary>
/// The canonical config document (D-I): <c>{ version, include, sources, pipelines, tables }</c>.
/// Definitions only — no ids, no runtime status, no timestamps/CreatedBy, no users/credentials.
/// <see cref="Include"/> lists relative paths to other documents that are composed BEFORE this
/// one (see <see cref="ConfigComposer.ComposeWithIncludes"/>) — this document (the includer)
/// always wins per-entity over anything its includes declare. <see cref="Sources"/> reuses
/// <see cref="SourceDefinition"/> directly (sources are already id-less — nothing to strip);
/// <see cref="Pipelines"/>/<see cref="Tables"/> use the trimmed <see cref="ConfigPipeline"/>/
/// <see cref="ConfigTable"/> shapes below.
/// </summary>
public sealed class ConfigDocument
{
    public int Version { get; set; } = 1;
    public List<string> Include { get; set; } = [];
    public List<SourceDefinition> Sources { get; set; } = [];
    public List<ConfigPipeline> Pipelines { get; set; } = [];
    public List<ConfigTable> Tables { get; set; } = [];

    /// <summary>Plan 016 wave 3 — the promotion-side half of the permissiveness split. Interactive
    /// editing is permissive by default (<c>PUT /api/sources/{name}</c> allows a breaking field change
    /// unless the caller opts into <c>?allowBreaking=false</c>); IMPORT is the opposite, because
    /// promoting a document into an environment is not the moment to discover that a generated client
    /// three deployments away stopped decoding. Null or absent means <c>"compatible"</c> — the gate is
    /// ON — so a document written before this field, or by a tool that never heard of it, is gated. Only
    /// the explicit string <c>"any"</c> turns it off, which is the one direction a typo must not take:
    /// <c>schemaPolicy: "compatable"</c> leaves enforcement on rather than silently disabling it, the
    /// same rule <c>Auth:Mode</c> follows.</summary>
    public string? SchemaPolicy { get; set; }

    /// <summary>Plan 016 wave 4 — "which connector kinds, at which versions, does this document need to
    /// import correctly". Empty (the default) declares nothing, so a document written before this field
    /// existed is unaffected. Checked at import by
    /// <c>ConfigImportService.DetectUnsatisfiedPluginRequirements</c> against
    /// <see cref="StreamsForge.AppCore.Transports.KindVersions.All"/> — see that method's doc comment for
    /// the fatal-vs-warning argument, and <see cref="StreamsForge.AppCore.Transports.SemVerRange"/> for the
    /// range grammar <see cref="ConfigPluginRequirement.Version"/> accepts.</summary>
    public List<ConfigPluginRequirement> Requires { get; set; } = [];
}

/// <summary>One entry of <see cref="ConfigDocument.Requires"/>: "this document needs KIND at a version
/// satisfying RANGE". Unlike <see cref="EntityPin"/> (which names a catalog ENTITY by name) this names a
/// connector KIND — a build-time capability of the instance, not something the catalog holds — so
/// there is no id/name-resolution question here, only "is it registered, and at what version".</summary>
public sealed class ConfigPluginRequirement
{
    /// <summary>A <c>SourceDefinition.Kind</c> / <c>SinkSpec.Kind</c> value, e.g. "postgres-cdc".</summary>
    public string Kind { get; set; } = "";

    /// <summary>A <see cref="StreamsForge.AppCore.Transports.SemVerRange"/> expression, e.g. "^2.0.0" or
    /// ">=1.2.0 &lt;2.0.0". Empty or "*" (the default) means "any version — just be present".</summary>
    public string Version { get; set; } = "*";
}

/// <summary>
/// A PIPELINE definition inside a config document (D-I). Deliberately missing everything that's
/// runtime/catalog state rather than configuration: no id, no <see cref="PipelineStatus"/>
/// (replaced by the boolean <see cref="Running"/> — the DESIRED state), no Error, no
/// CreatedBy/CreatedAtMs/UpdatedAtMs. A catalog pipeline whose <see cref="PipelineDefinition.Status"/>
/// is <see cref="PipelineStatus.Failed"/> exports as <c>Running == true</c> — it was asked to run
/// and hasn't been asked to stop; only <see cref="PipelineStatus.Stopped"/> maps to <c>false</c>.
/// See <see cref="ConfigSerializer.FromCatalog"/> for the exact mapping.
/// </summary>
public sealed class ConfigPipeline
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public string Sql { get; set; } = "";
    public bool Running { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, string> Metadata { get; set; } = [];
    /// <summary>Plan 009 B2: mirrors <see cref="PipelineDefinition.Sinks"/> (additive). NatsPubConfig
    /// credentials are masked (<see cref="SourceKinds.SecretMask"/>) in an export unless the caller asks
    /// for <c>includeSecrets</c> — see <see cref="ConfigSerializer.FromCatalog"/> — following the exact
    /// convention Sources' secrets already use in this document.</summary>
    public List<SinkSpec> Sinks { get; set; } = [];

    /// <summary>Plan 016 wave 3: mirrors <see cref="PipelineDefinition"/>.DependsOn — what this entity was
    /// authored against, as {kind, name, schemaRevision} pins. Empty (the default) is pruned from an
    /// export exactly like every other empty list here, so a document that declares no pins is byte-
    /// identical to one written before this field existed.</summary>
    public List<EntityPin> DependsOn { get; set; } = [];

    /// <summary>Plan 026 wave 3: mirrors <see cref="PipelineDefinition.ReplayFrom"/> — keyed by input
    /// name, where each stream input starts when this pipeline (re)starts. Empty (the default) is pruned
    /// from an export exactly like every other empty collection here. Carrying it here is what closes
    /// wave 2's found-and-not-fixed gap: without it, editing only <c>replayFrom</c> compared equal under
    /// <c>CatalogRevisions.PipelineCanonicalText</c> and never bumped <c>Revision</c> even though the
    /// entity really did restart.</summary>
    public Dictionary<string, ReplayFrom> ReplayFrom { get; set; } = [];

    /// <summary>Plan 026 wave 3: mirrors <see cref="PipelineDefinition.Replay"/> — disk-backed retention
    /// for this pipeline's result-batch log. Null (the default) is pruned from an export like any other
    /// null field.</summary>
    public ReplayConfig? Replay { get; set; }
}

/// <summary>
/// A TABLE definition inside a config document (D-I) — same shape as <see cref="ConfigPipeline"/>
/// plus the table-only configuration knobs (search + row-history + parallelism), mirroring
/// <see cref="TableDefinition"/>'s user-configurable surface. Runtime/derived fields are dropped:
/// no id, Status/Error, CreatedBy/timestamps, OutputFields, StreamInputs/TableInputs (those are
/// products of the last successful SQL compile, not configuration — re-derived on import when the
/// SQL is recompiled). <see cref="SearchMode"/>/<see cref="HistoryMode"/> carry the source enum's
/// C# member NAME as a string (e.g. "Exact"/"Fuzzy", "All"/"LastN"/"FirstN"/"MinBy"/"MaxBy") — see
/// <see cref="ConfigSerializer"/> for the exact wire shape and round-trip rule.
/// </summary>
public sealed class ConfigTable
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public string Sql { get; set; } = "";
    public bool Running { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, string> Metadata { get; set; } = [];
    public bool SearchEnabled { get; set; }
    public string SearchMode { get; set; } = "Exact";
    public bool HistoryEnabled { get; set; }
    public string HistoryMode { get; set; } = "All";
    public int HistoryLimit { get; set; } = 10;
    public string? HistoryByField { get; set; }
    public long HistoryWindowMs { get; set; }
    public int Parallelism { get; set; } = 1;
    /// <summary>Plan 009 B2: mirrors <see cref="TableDefinition.Sinks"/> — see the identical note on
    /// <see cref="ConfigPipeline.Sinks"/>.</summary>
    public List<SinkSpec> Sinks { get; set; } = [];

    /// <summary>Plan 011 C2: mirrors <see cref="TableDefinition.RetentionMaxRows"/>. Carried through
    /// export/import — unlike the purely operational Persistence/FlushMs knobs, which this document
    /// deliberately does not carry, retention changes what ROWS the table holds, so a config round-trip
    /// that dropped it would promote a differently-behaving table.</summary>
    public int RetentionMaxRows { get; set; }

    /// <summary>Plan 011 C2: mirrors <see cref="TableDefinition.RetentionTtlMs"/>.</summary>
    public long RetentionTtlMs { get; set; }

    /// <summary>Plan 011 D1: mirrors <see cref="TableDefinition.ShardBy"/>. Carried through export/import
    /// for the same reason retention is: it is not an operational knob but part of what the table IS —
    /// where its per-key state and history live, and (via the searchEnabled refusal) what else it can be
    /// configured with. A round-trip that dropped it would promote a table whose per-key lookups silently
    /// stopped existing. Empty = not sharded, so an older document imports unchanged.</summary>
    public List<string> ShardBy { get; set; } = [];

    /// <summary>Plan 016 wave 3: mirrors <see cref="TableDefinition"/>.DependsOn — what this entity was
    /// authored against, as {kind, name, schemaRevision} pins. Empty (the default) is pruned from an
    /// export exactly like every other empty list here, so a document that declares no pins is byte-
    /// identical to one written before this field existed.</summary>
    public List<EntityPin> DependsOn { get; set; } = [];

    /// <summary>Plan 026 wave 3: mirrors <see cref="TableDefinition.ReplayFrom"/> — keyed by input name,
    /// where each source/pipeline input starts when this table (re)starts. Empty (the default) is pruned
    /// from an export like every other empty collection here — see the identical note on
    /// <see cref="ConfigPipeline.ReplayFrom"/> for why this closes wave 2's found-and-not-fixed gap.</summary>
    public Dictionary<string, ReplayFrom> ReplayFrom { get; set; } = [];

    /// <summary>Plan 026 wave 3: mirrors <see cref="TableDefinition.Replay"/> — disk-backed retention for
    /// this table's delta-batch log. Null (the default) is pruned from an export like any other null
    /// field.</summary>
    public ReplayConfig? Replay { get; set; }
}
