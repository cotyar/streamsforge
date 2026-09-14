using System.Text.Json.Serialization;

namespace StreamsForge.Abstractions;

[GenerateSerializer]
public enum FieldType { String, Double, Long, Bool, Timestamp, Json }

/// <summary>A source field. <see cref="Children"/> is the declared nested shape of a
/// <see cref="FieldType.Json"/> field (drill-down schema) — metadata that documents the payload,
/// drives synthetic generation for the "generic" profile, and feeds editor autocomplete. Null/empty
/// for scalar fields.
///
/// <para><see cref="IsArray"/> (additive, default false): the field holds a JSON array rather than a
/// single value. Combined with the other two: IsArray + <see cref="Children"/> declared = a typed list
/// of records (each element shaped like <see cref="Children"/>) — DescriptorFactory emits a repeated
/// nested message. IsArray + no Children (and Type != Json) = a repeated scalar of <see cref="Type"/>.
/// IsArray + Type == Json + no Children = a repeated schemaless value — DescriptorFactory emits
/// repeated google.protobuf.Struct. Orthogonal to Type/Children, so every existing combination keeps
/// its current (non-array) meaning.</para></summary>
// NOTE (005-W1): written as a record with body-declared properties (plain `set`) rather than the
// original positional-record shorthand (which synthesizes `init`-only properties). Orleans'
// cross-assembly codegen path (GenerateCodeForDeclaringAssembly, used because this type now lives in
// shared/StreamsForge.Contracts while the generator runs in StreamsForge.Abstractions) resolves
// property accessors purely from metadata and doesn't recognize `init` as settable there (ORLEANS0101:
// "does not have an accessible setter") — the same constructor-matching heuristic that lets same-
// assembly codegen use positional records apparently isn't available across that boundary. Equality/
// ToString/deconstruction/`with` all still work identically (records synthesize those from every
// public instance property regardless of positional-vs-body declaration); only init-vs-set changed,
// which Orleans serialization never observed either way. No caller depends on init-only-ness (verified:
// every construction site uses `new FieldDef(...)`, none use object-initializer-only patterns that
// require init).
[GenerateSerializer]
public sealed record FieldDef
{
    [Id(0)] public string Name { get; set; }
    [Id(1)] public FieldType Type { get; set; }
    [Id(2)] public List<FieldDef>? Children { get; set; }
    [Id(3)] public bool IsArray { get; set; }

    public FieldDef(string Name, FieldType Type, List<FieldDef>? Children = null, bool IsArray = false)
    {
        this.Name = Name;
        this.Type = Type;
        this.Children = Children;
        this.IsArray = IsArray;
    }
}

/// <summary>A stream source: schema + synthetic generator settings.</summary>
[GenerateSerializer]
public sealed class SourceDefinition
{
    [Id(0)] public string Name { get; set; } = "";
    [Id(1)] public string Description { get; set; } = "";
    [Id(2)] public List<FieldDef> Fields { get; set; } = [];
    /// <summary>Generator profile: "trades" | "quotes" | "orders" | "generic" | "scenario" (wishlist
    /// #8 — see <see cref="GeneratorProfiles.Scenario"/> and <see cref="ScenarioSpec"/>) | a handful of
    /// other literal strings <see cref="MarketDataProfiles.GenerateEvent"/> switches on directly
    /// ("json-events", "multileg", "lifecycle" — never promoted to constants here, only the newest one
    /// is, to keep this doc comment from becoming its own maintenance burden).</summary>
    [Id(3)] public string GeneratorProfile { get; set; } = "generic";
    [Id(4)] public double EventsPerSecond { get; set; } = 5;
    [Id(5)] public bool Enabled { get; set; } = true;
    /// <summary>User-editable free-form labels — see Feature A (metadata) in TableDefinition's doc comment.</summary>
    [Id(6)] public List<string> Tags { get; set; } = [];
    /// <summary>User-editable free-form key-value annotations.</summary>
    [Id(7)] public Dictionary<string, string> Metadata { get; set; } = [];
    /// <summary>Source kind (plan 006, additive): "generator" (default — the pre-existing
    /// behavior) | "url" | "file" | "folder" | "grpc". See <see cref="SourceKinds"/>.</summary>
    [Id(8)] public string Kind { get; set; } = SourceKinds.Generator;
    /// <summary>Connector configuration; null for generator-kind sources (plan 006).</summary>
    [Id(9)] public ConnectorConfig? Connector { get; set; }
    /// <summary>Client-push ingress configuration (plan 008 W4); non-null only for
    /// <see cref="SourceKinds.Ingest"/> sources.</summary>
    [Id(10)] public IngestConfig? Ingest { get; set; }
    /// <summary>Plan 009 C2: what an inbound row does when a value cannot be coerced to its declared
    /// field type. Applies to EVERY inbound path — push ingress, the four connector kinds, and NATS —
    /// so a stringly-typed feed can be declared with real types and parsed on arrival. Default
    /// <see cref="CoercionFailurePolicy.Null"/> preserves the pre-009 lenient behavior.</summary>
    [Id(11)] public CoercionFailurePolicy OnCoercionFailure { get; set; } = CoercionFailurePolicy.Null;
    /// <summary>Wishlist #8: non-null only for <see cref="GeneratorProfiles.Scenario"/>-profile sources.
    /// EventsPerSecond is ignored (must be 0, by convention — nothing enforces it, mirroring how e.g. a
    /// "url"-kind source's EventsPerSecond is simply unused) for this profile: rows are produced only by
    /// an explicit <c>POST /api/sources/{name}/run</c>, never by GeneratorGrain's tick timer.</summary>
    [Id(12)] public ScenarioSpec? Scenario { get; set; }
    /// <summary>Plan 015: who last changed this definition. CreatedBy has always been recorded; the
    /// counterpart was not, so "who broke prod" was unanswerable from the catalog alone. Set by
    /// CatalogRecordMerge's 4-arg overload on every update; empty on records last written before 015.</summary>
    [Id(13)] public string UpdatedBy { get; set; } = "";

    // --------------------------------------------------------------------------------------------
    // Plan 016 wave 0 — pre-built so wave 1's three concurrent agents never meet in this file.
    //
    // BOTH counters are REGISTRY-ASSIGNED and never client-settable: an incoming definition's values are
    // discarded exactly the way CreatedAtMs already is (CatalogRecordMerge.CarryServerOwnedFields), or a
    // caller could pin themselves to a revision they invented. Zero on records written before 016.
    // --------------------------------------------------------------------------------------------

    /// <summary>Monotonic, bumped whenever the stored definition actually changes — "changed" being
    /// canonical-JSON inequality, the same predicate ImportPlanner already uses to tell "skipped" from
    /// "updated", so a round-trip import that reports "skipped" provably does not bump this.</summary>
    [Id(14)] public long Revision { get; set; }

    /// <summary>Monotonic, bumped ONLY when the field shape changes — not when a knob does. That split is
    /// the entire reason a pin is useful: an eventsPerSecond edit must not invalidate a downstream
    /// dependant, and without two counters the choice is between pins that fire constantly and pins that
    /// never fire.</summary>
    [Id(15)] public long SchemaRevision { get; set; }

    /// <summary>Plan 021 D5 — the environment this entity belongs to, empty for the default one. Written
    /// ONCE at creation from the request's environment and never edited afterwards: the name is in every
    /// runtime key this entity owns, so changing it would strand a grain, a state file and a stream the
    /// same way renaming a sharded table would.
    ///
    /// <para>It exists because the ambient (<c>EnvironmentAmbient</c>) answers "which catalog is this
    /// REQUEST talking to" and is empty everywhere else. Supervisors, the lifecycle orchestrator, connector
    /// drivers and stream bridges run on timers and subscriptions, outside any request — they read this
    /// field. Conflating the two is how background work silently operates on <c>default</c>.</para>
    ///
    /// <para>Deliberately NOT part of a config document (D8): a document carrying its environment would be
    /// deployable to exactly one place, which is the opposite of the point. The environment is a property
    /// of the import CALL. Config export therefore omits it, and import writes it from the target.</para></summary>
    [Id(16)] public string Environment { get; set; } = "";
}

/// <summary>Well-known <see cref="SourceDefinition.GeneratorProfile"/> values that have a dedicated
/// contract type behind them (unlike "trades"/"quotes"/etc., which are opaque strings
/// <see cref="MarketDataProfiles.GenerateEvent"/> switches on) — string constants rather than an enum,
/// mirroring <c>SourceKinds</c>'s own additive-without-renumbering rationale in ConnectorModels.cs.</summary>
public static class GeneratorProfiles
{
    /// <summary>Wishlist #8: a parametric, seedable multi-path/multi-instrument Monte-Carlo-style scenario
    /// batch, run on demand rather than ticked. See <see cref="ScenarioSpec"/>.</summary>
    public const string Scenario = "scenario";
}

// ============================================================================
// Wishlist #8 — parametric, seedable scenario generator.
//
// SHAPE: Paths (N) independent Monte Carlo paths x Instruments (K) x Days (D) = N*K*D rows, one shock/
// value per (path, instrument, day). Determinism is the entire point (see AppCore/Generators/
// ScenarioGenerator.cs's class doc for the exact RNG-ordering contract that makes "same seed -> byte-
// identical batch" true): every run seeds its OWN System.Random from ScenarioSpec.Seed (or the request's
// override), NEVER Random.Shared, which is exactly what MarketDataProfiles.GenerateEvent uses for every
// OTHER profile and is exactly what a seedable, reproducible source cannot share.
//
// CORRELATION: one common factor per Group plus idiosyncratic noise, mixed by Rho (single-factor model:
// standardizedShock = sqrt(Rho)*groupFactor + sqrt(1-Rho)*idiosyncratic — both terms drawn from the same
// Distribution, both variance 1, so Corr(shock_i, shock_j) = Rho for any two instruments i != j sharing a
// Group, in the large-sample limit). An instrument with no Group (blank/null) is its own singleton group,
// so Rho has no observable effect on it (matches the wishlist's rho=0/rho=1 sanity checks).
//
// TOTAL: ScenarioSpec.Validate() enumerates EVERY config problem (never throws, never stops at the first
// one) so a caller sees the whole list in one 400, and ScenarioGenerator.GenerateBatch NEVER throws for a
// bad spec/request — a bad config is always a ScenarioRunResult with Outcome != Accepted, Rows empty,
// Errors populated. MaxBatchRows caps Paths*Instruments.Count*Days for ANY run (including overrides);
// exceeding it is one more Validate() entry, not a truncated batch and not an exception mid-emit.
//
// KNOWN GAP (documented, not silently dropped): the wishlist allows Instruments to be either an inline
// list or "a reference to a source/table name". Only the inline list is implemented — a spec that sets
// InstrumentsSourceName always fails validation with an explicit message (see ScenarioSpec.Validate)
// rather than being ignored or silently falling back to an empty instrument set.
// ============================================================================

/// <summary>The marginal shape of a scenario run's standardized shocks (mean 0, variance 1 before Vol
/// scaling) — see <see cref="ScenarioSpec.Distribution"/>. A string Kind rather than an enum, same
/// additive-without-renumbering reasoning as <see cref="GeneratorProfiles"/>.</summary>
[GenerateSerializer]
public sealed class ScenarioDistributionSpec
{
    /// <summary>"normal" | "lognormal" | "student_t" (case-insensitive). Anything else is a validation
    /// error, never a silent fallback to "normal".</summary>
    [Id(0)] public string Kind { get; set; } = "normal";
    /// <summary>Degrees of freedom — "student_t" only, ignored otherwise. Must be &gt; 2: at df &lt;= 2
    /// the Student-t distribution's variance is infinite/undefined, so "standardize to variance 1" (what
    /// ScenarioGenerator does to every distribution so Vol means the same thing regardless of Kind) would
    /// itself be undefined.</summary>
    [Id(1)] public double Df { get; set; } = 5;
}

/// <summary>One instrument in a scenario spec's inline instrument list.</summary>
[GenerateSerializer]
public sealed class ScenarioInstrumentSpec
{
    [Id(0)] public string Id { get; set; } = "";
    /// <summary>Day-0 level. Must be &gt; 0 when the spec's Distribution is "lognormal" (a multiplicative
    /// process starting at/below 0 has no meaningful next value) — enforced by ScenarioSpec.Validate, not
    /// by clamping at generation time.</summary>
    [Id(1)] public double Base { get; set; }
    /// <summary>Per-day volatility applied to the (already unit-variance) mixed standardized shock.</summary>
    [Id(2)] public double Vol { get; set; }
    /// <summary>Correlation group name. Instruments sharing a Group share one common factor draw per
    /// (path, day) — see this file's Wishlist #8 class doc. Blank/null = its own singleton group.</summary>
    [Id(3)] public string Group { get; set; } = "";
}

/// <summary>Wishlist #8: the persisted, reusable definition of one scenario source's generator —
/// everything a run needs except the per-run RunId/Seed-override/Overrides, which arrive on
/// <see cref="ScenarioRunRequest"/> instead so the same spec can be replayed with different knobs without
/// mutating the catalog entry.</summary>
[GenerateSerializer]
public sealed class ScenarioSpec
{
    /// <summary>N: number of independent Monte Carlo paths. Must be &gt; 0.</summary>
    [Id(0)] public int Paths { get; set; } = 1;
    /// <summary>K: instruments, declared inline (see this file's Wishlist #8 class doc for why this is
    /// the only supported form today). Must be non-empty with unique, non-blank Ids.</summary>
    [Id(1)] public List<ScenarioInstrumentSpec> Instruments { get; set; } = [];
    /// <summary>KNOWN GAP: reserved for the source/table-reference form of Instruments the wishlist also
    /// describes. Always rejected by Validate (never silently ignored) — see this file's class doc.</summary>
    [Id(2)] public string? InstrumentsSourceName { get; set; }
    [Id(3)] public ScenarioDistributionSpec Distribution { get; set; } = new();
    /// <summary>Common-factor correlation weight, must be in [0,1]. 0 = every instrument moves off its own
    /// idiosyncratic draw only (its Group is decorative); 1 = every instrument in a Group is driven by the
    /// identical daily factor.</summary>
    [Id(4)] public double Rho { get; set; }
    /// <summary>D: horizon in days. 1 = a single shock (rows carry Day == 1 only, no Day == 0 baseline
    /// row — see ScenarioGenerator, this keeps the row COUNT exactly N*K*D as the wishlist states it).
    /// Must be &gt; 0.</summary>
    [Id(5)] public int Days { get; set; } = 1;
    /// <summary>Default seed used when a run request doesn't override one. 0 is a legitimate seed, not
    /// "unset" — a run always has a concrete seed, spec default or request override, never a process-
    /// random one (this is what makes "same seed -> identical batch" possible at all).</summary>
    [Id(6)] public long Seed { get; set; }
    /// <summary>Hard cap on Paths*Instruments.Count*Days for ANY run of this spec, including overrides.
    /// Exceeding it is a Validate() error — never a partial/truncated emit — so "the batch" in the
    /// wishlist's row contract always means the WHOLE batch or nothing. Named/shaped after
    /// <see cref="IngestConfig.MaxBatchRows"/> but enforced at spec-validation time rather than at buffer-
    /// admission time: a scenario run has no shared buffer to admit into (see GeneratorGrain.RunAsync's
    /// doc comment for how "honouring backpressure" is interpreted here instead).</summary>
    [Id(7)] public int MaxBatchRows { get; set; } = 100_000;

    /// <summary>Every problem with this spec, TOTAL (never throws, never short-circuits after the first
    /// finding) — <paramref name="effectivePaths"/>/<paramref name="effectiveDays"/>/
    /// <paramref name="effectiveRho"/>/<paramref name="effectiveDistribution"/> are the values AFTER a
    /// run request's overrides are applied (see <see cref="ScenarioRunOverrides"/>), so a run-time
    /// override that breaks the spec is caught here too, not just the stored defaults.</summary>
    public List<string> Validate(int effectivePaths, int effectiveDays, double effectiveRho, ScenarioDistributionSpec effectiveDistribution)
    {
        var errors = new List<string>();

        if (effectivePaths <= 0)
        {
            errors.Add("scenario.paths must be > 0");
        }

        if (effectiveDays <= 0)
        {
            errors.Add("scenario.days must be > 0");
        }

        if (double.IsNaN(effectiveRho) || effectiveRho < 0 || effectiveRho > 1)
        {
            errors.Add("scenario.rho must be within [0,1]");
        }

        if (!string.IsNullOrEmpty(InstrumentsSourceName))
        {
            errors.Add("scenario.instrumentsSourceName (reference-based instruments) is not supported yet — use an inline scenario.instruments list");
        }

        if (Instruments.Count == 0)
        {
            errors.Add("scenario.instruments must be non-empty");
        }
        else
        {
            var seenIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var instrument in Instruments)
            {
                if (string.IsNullOrWhiteSpace(instrument.Id))
                {
                    errors.Add("scenario.instruments[].id must be non-blank");
                }
                else if (!seenIds.Add(instrument.Id))
                {
                    errors.Add($"scenario.instruments[].id '{instrument.Id}' is duplicated");
                }

                if (instrument.Vol < 0)
                {
                    errors.Add($"scenario.instruments['{instrument.Id}'].vol must be >= 0");
                }
            }
        }

        var kind = effectiveDistribution.Kind?.Trim().ToLowerInvariant();
        if (kind is not ("normal" or "lognormal" or "student_t"))
        {
            errors.Add("scenario.distribution.kind must be 'normal', 'lognormal', or 'student_t'");
        }
        else if (kind == "student_t" && effectiveDistribution.Df <= 2)
        {
            errors.Add("scenario.distribution.df must be > 2 for student_t (variance is undefined at or below 2 degrees of freedom)");
        }
        else if (kind == "lognormal")
        {
            foreach (var instrument in Instruments)
            {
                if (instrument.Base <= 0)
                {
                    errors.Add($"scenario.instruments['{instrument.Id}'].base must be > 0 when scenario.distribution.kind is 'lognormal'");
                }
            }
        }

        if (MaxBatchRows <= 0)
        {
            errors.Add("scenario.maxBatchRows must be > 0");
        }
        else if (errors.Count == 0)
        {
            // Only meaningful once paths/days/instruments are individually sane — an already-invalid
            // spec would otherwise also report a confusing "0 rows exceeds maxBatchRows" or similar.
            var totalRows = (long)effectivePaths * Instruments.Count * effectiveDays;
            if (totalRows > MaxBatchRows)
            {
                errors.Add($"scenario run would emit {totalRows} rows (paths={effectivePaths} x instruments={Instruments.Count} x days={effectiveDays}), exceeding scenario.maxBatchRows ({MaxBatchRows})");
            }
        }

        return errors;
    }
}

/// <summary>Wishlist #8: a run request's optional per-run overrides — everything else (Instruments,
/// InstrumentsSourceName, MaxBatchRows, and the spec's default Seed) always comes from the stored
/// <see cref="ScenarioSpec"/>; deliberately NOT extended to override those too, so MaxBatchRows stays an
/// honest cap a caller cannot raise from the request path, and Instruments stays a catalog-time concern.</summary>
[GenerateSerializer]
public sealed class ScenarioRunOverrides
{
    [Id(0)] public int? Paths { get; set; }
    [Id(1)] public int? Days { get; set; }
    [Id(2)] public double? Rho { get; set; }
    [Id(3)] public ScenarioDistributionSpec? Distribution { get; set; }
}

/// <summary>Body of <c>POST /api/sources/{name}/run</c>. JsonPropertyName pins the wire shape to the
/// wishlist's literal <c>{ run_id, seed?, overrides? }</c> — snake_case here (unlike the rest of this
/// API's camelCase-by-default bodies) specifically to match the row contract's own field names
/// (<see cref="ScenarioRow"/>), since a caller round-tripping RunId between a run request and the rows it
/// produced should see the identical spelling in both places.</summary>
[GenerateSerializer]
public sealed class ScenarioRunRequest
{
    /// <summary>Caller-supplied identity for this run, stamped onto every emitted row's RunId. Required —
    /// it's the field a "before/after a CSA amendment" comparison joins two runs' rows on.</summary>
    [Id(0)]
    [JsonPropertyName("run_id")]
    public string RunId { get; set; } = "";
    /// <summary>Overrides <see cref="ScenarioSpec.Seed"/> for this run only. Null = use the spec's stored
    /// default seed. Either way the run has a concrete seed — see ScenarioSpec.Seed's own doc comment.</summary>
    [Id(1)] public long? Seed { get; set; }
    [Id(2)] public ScenarioRunOverrides? Overrides { get; set; }

    /// <summary>Wishlist #9(b): when true, emit only ONE day — the next unemitted day of THIS RunId's
    /// sequence — instead of the whole D-day batch. The generator (GeneratorGrain/GeneratorActor) keeps
    /// per-RunId continuation state in memory (see <c>ScenarioGenerator.ScenarioRunState</c>,
    /// shared/StreamsForge.AppCore/Generators/ScenarioGenerator.cs) so repeated <c>step: true</c> calls
    /// with the SAME RunId walk day 1, then 2, then 3, … — this is what makes a path-dependent
    /// simulation possible: step t+1 can only be requested (and only makes sense) after the caller has
    /// finished reacting to step t's rows, which is exactly the ordering a tick-driven generator cannot
    /// offer. <see cref="Seed"/>/<see cref="Overrides"/> are read ONLY on the first <c>step: true</c> call
    /// for a given RunId (the one that begins the run); a later step call with the same RunId ignores
    /// them — the run's effective parameters are locked in at creation, exactly like a whole-batch run's
    /// are locked in for its one call. A step call once every day (1..D) has already been emitted is NOT
    /// an error: it returns <see cref="ScenarioRunOutcome.Accepted"/> with <c>Accepted == 0</c> and an
    /// empty <see cref="ScenarioRunResult.Rows"/> — a no-op, not a new outcome value, so the REST layer
    /// (shared/StreamsForge.Api/Endpoints/SourceRunEndpoints.cs, out of this change's file-ownership scope)
    /// needs no change to answer it correctly. Determinism contract: stepping day-by-day for a RunId
    /// produces BYTE-IDENTICAL rows to a single non-step call with the same effective seed/spec — both
    /// walk the identical per-day code path (<c>ScenarioGenerator.GenerateDay</c>); see
    /// ScenarioGeneratorSteppingTests for the equivalence test that pins this.</summary>
    [Id(3)] public bool Step { get; set; }
}

/// <summary>One emitted row — the wishlist's exact row contract (run_id/path_id/instrument_id/day/factor/
/// shock/value), plus TsMs. TsMs is supplied by the CALLER (GenerateBatch takes it as a parameter, never
/// reads the clock itself) specifically so the deterministic fields can be tested byte-for-byte without
/// also having to control wall-clock time — see ScenarioGenerator.GenerateBatch's doc comment.</summary>
[GenerateSerializer]
public sealed class ScenarioRow
{
    [Id(0)] [JsonPropertyName("run_id")] public string RunId { get; set; } = "";
    [Id(1)] [JsonPropertyName("path_id")] public long PathId { get; set; }
    [Id(2)] [JsonPropertyName("instrument_id")] public string InstrumentId { get; set; } = "";
    [Id(3)] [JsonPropertyName("day")] public long Day { get; set; }
    /// <summary>The (path, day, group) common-factor draw actually used for this row's instrument —
    /// always drawn even when Rho == 0 (keeps the RNG draw SEQUENCE, and therefore every OTHER row's
    /// values, identical regardless of Rho; only the mixing weight differs). Standardized (mean 0,
    /// variance 1) — not yet scaled by Vol.</summary>
    [Id(4)] [JsonPropertyName("factor")] public double Factor { get; set; }
    /// <summary>The mixed, standardized shock actually applied: sqrt(Rho)*Factor + sqrt(1-Rho)*idiosyncratic.
    /// Mean 0, variance 1 — not yet scaled by Vol; see Value for the scaled/evolved result.</summary>
    [Id(5)] [JsonPropertyName("shock")] public double Shock { get; set; }
    /// <summary>This instrument's level after Day days of evolution from its Base — additive
    /// (Base + sum of Vol*Shock) for "normal"/"student_t", multiplicative (Base * product of
    /// exp(Vol*Shock - Vol^2/2)) for "lognormal". See ScenarioGenerator.</summary>
    [Id(6)] [JsonPropertyName("value")] public double Value { get; set; }
    /// <summary>Wire name "_ts" — the wishlist's row contract lists this as "(+ `_ts`)", matching
    /// <c>EventRecord.TimestampField</c>'s literal reserved-key spelling exactly (see
    /// ScenarioGenerator.ToEventRecord, which stamps this value onto that same key).</summary>
    [Id(7)] [JsonPropertyName("_ts")] public long TsMs { get; set; }
}

public enum ScenarioRunOutcome
{
    /// <summary>The whole batch was generated; Accepted == Rows.Count == Paths*Instruments.Count*Days.</summary>
    Accepted = 0,
    /// <summary>The spec/request failed ScenarioSpec.Validate — see Errors. Rows is empty; nothing was
    /// emitted (never a partial batch).</summary>
    ValidationError = 1,
    /// <summary>No such source, or the source has never been started (no SourceDefinition on file for
    /// this generator activation).</summary>
    NotFound = 2,
    /// <summary>The source exists but its GeneratorProfile isn't <see cref="GeneratorProfiles.Scenario"/>
    /// (or its Scenario spec is null) — mirrors IngestOutcome.WrongKind's reasoning: running a non-
    /// scenario generator on demand would make its tick-driven semantics unreconcilable.</summary>
    WrongProfile = 3,
}

/// <summary>Result of one <c>POST /api/sources/{name}/run</c> — the wishlist's literal
/// <c>{ accepted, rows }</c> response shape, plus Outcome/Errors for the non-Accepted cases.</summary>
[GenerateSerializer]
public sealed class ScenarioRunResult
{
    [Id(0)] public ScenarioRunOutcome Outcome { get; set; }
    /// <summary>Rows.Count when Outcome == Accepted; 0 otherwise.</summary>
    [Id(1)] public int Accepted { get; set; }
    [Id(2)] public List<ScenarioRow> Rows { get; set; } = [];
    /// <summary>Populated for ValidationError; one entry per independent problem (TOTAL — see
    /// ScenarioSpec.Validate).</summary>
    [Id(3)] public List<string> Errors { get; set; } = [];
}

/// <summary>Plan 009 C2: what happens to a value that will not coerce to its declared field type.
/// Whichever is chosen, the failure is COUNTED and surfaced — a silently vanishing row is the one
/// outcome none of these may produce.</summary>
public enum CoercionFailurePolicy
{
    /// <summary>The field becomes null, the rest of the row is kept. Lenient, and the default because
    /// it is the pre-009 behavior.</summary>
    Null,
    /// <summary>The whole row is discarded. Use when a partly-null row is worse than no row.</summary>
    DropRow,
    /// <summary>The whole batch is refused (a 400 on the push path). Strictest; keeps a malformed feed
    /// from being half-ingested.</summary>
    RejectBatch,
}

[GenerateSerializer]
public enum PipelineStatus { Stopped, Running, Failed }

[GenerateSerializer]
public sealed class PipelineDefinition
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    [Id(2)] public string Description { get; set; } = "";
    [Id(3)] public string Sql { get; set; } = "";
    [Id(4)] public PipelineStatus Status { get; set; } = PipelineStatus.Stopped;
    [Id(5)] public string? Error { get; set; }
    [Id(6)] public string CreatedBy { get; set; } = "";
    [Id(7)] public long CreatedAtMs { get; set; }
    [Id(8)] public long UpdatedAtMs { get; set; }
    /// <summary>User-editable free-form labels — see Feature A (metadata) in TableDefinition's doc comment.</summary>
    [Id(9)] public List<string> Tags { get; set; } = [];
    /// <summary>User-editable free-form key-value annotations.</summary>
    [Id(10)] public Dictionary<string, string> Metadata { get; set; } = [];

    /// <summary>Plan 008: real leaf source names this pipeline reads, from the last successful compile —
    /// the pipeline-side counterpart of TableDefinition.StreamInputs/TableInputs, and what makes lineage
    /// readable without a compile round-trip (POST /api/pipelines/validate is Editor-gated, a lineage view
    /// is not). Derived, never user-editable; empty until the SQL compiles.</summary>
    [Id(11)] public List<string> SourceNames { get; set; } = [];

    /// <summary>Plan 009 B2: where this pipeline's result rows are republished. Empty = nowhere, the
    /// pre-009 behavior. Delivery is fire-and-forget — see <see cref="SinkSpec"/>.</summary>
    [Id(12)] public List<SinkSpec> Sinks { get; set; } = [];

    /// <summary>Plan 015: who last changed this definition. CreatedBy has always been recorded; the
    /// counterpart was not, so "who broke prod" was unanswerable from the catalog alone. Set by
    /// CatalogRecordMerge's 4-arg overload on every update; empty on records last written before 015.</summary>
    [Id(13)] public string UpdatedBy { get; set; } = "";

    // --------------------------------------------------------------------------------------------
    // Plan 016 wave 0 — pre-built so wave 1's three concurrent agents never meet in this file.
    //
    // BOTH counters are REGISTRY-ASSIGNED and never client-settable: an incoming definition's values are
    // discarded exactly the way CreatedAtMs already is (CatalogRecordMerge.CarryServerOwnedFields), or a
    // caller could pin themselves to a revision they invented. Zero on records written before 016.
    // --------------------------------------------------------------------------------------------

    /// <summary>Monotonic, bumped whenever the stored definition actually changes — "changed" being
    /// canonical-JSON inequality, the same predicate ImportPlanner already uses to tell "skipped" from
    /// "updated", so a round-trip import that reports "skipped" provably does not bump this.</summary>
    [Id(14)] public long Revision { get; set; }

    /// <summary>Plan 016: what this entity was authored against, checked at exactly two moments — config
    /// import (against the post-import world, so mode=validate catches it before anything is applied) and
    /// start. Deliberately NOT re-checked continuously: a pin broken by an upstream change sets
    /// <see cref="StaleReason"/> and is badged, while the entity keeps running on its compiled plan, which
    /// is what it does today — only now visibly.
    ///
    /// <para>Pins live here and in config documents, never in SQL. <c>FROM trades@3</c> would touch the
    /// tokenizer, parser, AST, validator, planner, editor autocomplete, formatter and highlighter — the
    /// most expensive change available, in the one project all work serializes on — and it would have no
    /// coherent runtime meaning, because the engine executes against live streams with no versioned store
    /// to read revision 3 out of.</para></summary>
    [Id(15)] public List<EntityPin> DependsOn { get; set; } = [];

    /// <summary>Why this entity's pins no longer hold, or null when they do. Set by the upstream change
    /// that broke them; cleared when the pins are re-satisfied. A string rather than a flag because the
    /// only useful thing to render is WHICH dependency moved and from what — a boolean would send the
    /// operator to the logs to learn the one fact the badge exists to convey.</summary>
    [Id(16)] public string? StaleReason { get; set; }

    /// <summary>Plan 021 D5 — the environment this entity belongs to, empty for the default one. Written
    /// ONCE at creation from the request's environment and never edited afterwards: the name is in every
    /// runtime key this entity owns, so changing it would strand a grain, a state file and a stream the
    /// same way renaming a sharded table would.
    ///
    /// <para>It exists because the ambient (<c>EnvironmentAmbient</c>) answers "which catalog is this
    /// REQUEST talking to" and is empty everywhere else. Supervisors, the lifecycle orchestrator, connector
    /// drivers and stream bridges run on timers and subscriptions, outside any request — they read this
    /// field. Conflating the two is how background work silently operates on <c>default</c>.</para>
    ///
    /// <para>Deliberately NOT part of a config document (D8): a document carrying its environment would be
    /// deployable to exactly one place, which is the opposite of the point. The environment is a property
    /// of the import CALL. Config export therefore omits it, and import writes it from the target.</para></summary>
    [Id(17)] public string Environment { get; set; } = "";

    /// <summary>Table-over-pipeline: this pipeline's compiled output schema — the exact counterpart of
    /// <see cref="TableDefinition.OutputFields"/>, filled from <c>CompileResult.OutputSchema</c> by
    /// <c>RegistryGrain.ApplyPipelineCompileResult</c> and cleared when the SQL does not compile.
    ///
    /// <para>It exists so a TABLE can name this pipeline as a relation: a relation needs a schema at
    /// compile time, and a pipeline had nowhere to keep one. Derived, never user-editable. Empty on every
    /// record written before this field existed — <c>EnsureInitializedAsync</c>'s backfill loop
    /// recompiles those, driven off the data (empty <c>OutputFields</c>) rather than off whether seeding
    /// just happened, exactly like the <see cref="SourceNames"/> backfill it extends.</para></summary>
    [Id(18)] public List<FieldDef> OutputFields { get; set; } = [];

    /// <summary>Plan 026 D5 (additive): where each STREAM input starts when this pipeline (re)starts,
    /// keyed by input (source) name — a producer position or an event time (see <see cref="ReplayFrom"/>).
    /// Applied at start only, when the executor is fresh: replaying into a live executor would make every
    /// replayed row a late event (the Engine drops rows older than its watermark minus 1 s), so changing
    /// this on a Running pipeline RESTARTS it, like an SQL edit. An input not named here attaches exactly as
    /// before (plan 023's default late-attach for connector sources; live-only for generators/ingest).
    /// Dapr stores it and refuses to START with it set (`dapr/PARITY.md` D11). Empty = off.</summary>
    [Id(19)] public Dictionary<string, ReplayFrom> ReplayFrom { get; set; } = [];
}

/// <summary>One emitted result row. Values are primitives only (string/double/long/bool/null).</summary>
[GenerateSerializer]
public sealed class ResultEnvelope
{
    [Id(0)] public string PipelineId { get; set; } = "";
    [Id(1)] public long Seq { get; set; }
    [Id(2)] public long TimestampMs { get; set; }
    [Id(3)] public Dictionary<string, object?> Row { get; set; } = [];
}

[GenerateSerializer]
public sealed class PipelineMetrics
{
    [Id(0)] public string PipelineId { get; set; } = "";
    [Id(1)] public PipelineStatus Status { get; set; }
    [Id(2)] public double EventsInPerSec { get; set; }
    [Id(3)] public double RowsOutPerSec { get; set; }
    [Id(4)] public long TotalEventsIn { get; set; }
    [Id(5)] public long TotalRowsOut { get; set; }
    [Id(6)] public long WindowsClosed { get; set; }
    [Id(7)] public long LastEventTsMs { get; set; }
    /// <summary>Plan 025: events the Engine discarded because their <c>_ts</c> was already behind the
    /// pipeline's watermark (1000 ms allowed lateness) — <c>PipelineExecutor.LateEvents</c>. This was
    /// invisible until a Dapr-side relay delay made a whole 50-row batch late and nothing anywhere said
    /// so; a growing count with <c>TotalRowsOut</c> flat is the signature.</summary>
    [Id(8)] public long LateEvents { get; set; }
}

/// <summary>Published on the lifecycle stream when a pipeline changes state.</summary>
[GenerateSerializer]
public sealed class LifecycleEvent
{
    [Id(0)] public string PipelineId { get; set; } = "";
    /// <summary>Which entity the event is about is encoded in the PREFIX, because all three share one
    /// stream and one <see cref="PipelineId"/> field:
    /// <list type="bullet">
    /// <item>pipelines (no prefix): "created" | "updated" | "deleted" | "started" | "stopped" | "failed" —
    /// <see cref="PipelineId"/> is the pipeline's id.</item>
    /// <item>tables ("table-"): "table-created" | "table-updated" | "table-deleted" | "table-started" |
    /// "table-stopped" | "table-failed" — <see cref="PipelineId"/> carries the table's NAME (its grain
    /// key), not an id.</item>
    /// <item>sources ("source-"): "source-started" | "source-stopped" | "source-deleted" —
    /// <see cref="PipelineId"/> carries the source's NAME, sources having no id at all. Started/stopped
    /// follow <c>SourceDefinition.Enabled</c> on every upsert (create AND update), so a subscriber sees
    /// one event per accepted write rather than only on a transition; <see cref="Status"/> is
    /// Running/Stopped to match. There is no "source-failed" — a source that fails to poll reports it
    /// through its own connector status, not on this stream.</item>
    /// </list>
    /// A subscriber that does not recognise a prefix must ignore the event, never fall through to the
    /// pipeline branch: this list grows additively.</summary>
    [Id(1)] public string Kind { get; set; } = "";
    [Id(2)] public PipelineStatus Status { get; set; }
    [Id(3)] public long TimestampMs { get; set; }
}

/// <summary>Per-table reverse-index search strategy — see StreamsForge.Host.Search.TableSearchIndex.</summary>
[GenerateSerializer]
public enum TableSearchMode { Exact, Fuzzy }

/// <summary>Per-key retention policy for opt-in ROW HISTORY (see TableDefinition.HistoryEnabled and
/// StreamsForge.Host.Grains.TableHistoryGrain). All: keep every version up to an internal safety cap.
/// LastN/FirstN: keep the most-recent/earliest N versions (ring buffer / stop-appending respectively).
/// MinBy/MaxBy: keep only the version with the min/max value of HistoryByField, plus the always-current
/// latest version (2 entries max).</summary>
[GenerateSerializer]
public enum TableHistoryMode { All, LastN, FirstN, MinBy, MaxBy }

/// <summary>A persistent materialized TABLE: a SELECT over streams and/or other tables, without windows
/// (running aggregates instead of windowed ones). Its name is unique across sources+tables and enters the
/// SQL namespace, so other tables can FROM/JOIN it directly.</summary>
[GenerateSerializer]
public sealed class TableDefinition
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    [Id(2)] public string Description { get; set; } = "";
    [Id(3)] public string Sql { get; set; } = "";
    [Id(4)] public PipelineStatus Status { get; set; } = PipelineStatus.Stopped;
    [Id(5)] public string? Error { get; set; }
    [Id(6)] public string CreatedBy { get; set; } = "";
    [Id(7)] public long CreatedAtMs { get; set; }
    [Id(8)] public long UpdatedAtMs { get; set; }
    /// <summary>Output row schema (name + kind) from the last successful compile — used to validate
    /// downstream tables that FROM/JOIN this one, independent of whether this table is currently Running.</summary>
    [Id(9)] public List<FieldDef> OutputFields { get; set; } = [];
    /// <summary>Stream source names this table's SQL reads from directly (from the last successful compile).</summary>
    [Id(10)] public List<string> StreamInputs { get; set; } = [];
    /// <summary>Other table names this table's SQL reads from directly (from the last successful compile).</summary>
    [Id(11)] public List<string> TableInputs { get; set; } = [];
    /// <summary>Whether a reverse (inverted) search index over this table's rows is maintained.</summary>
    [Id(12)] public bool SearchEnabled { get; set; }
    /// <summary>Exact (token/prefix/substring) or Fuzzy (trigram-similarity, typo-tolerant) search.</summary>
    [Id(13)] public TableSearchMode SearchMode { get; set; } = TableSearchMode.Exact;

    // ------------------------------------------------------------------
    // Feature B: opt-in per-row-identity version history. See TableHistoryGrain.
    // ------------------------------------------------------------------

    /// <summary>Whether a TableHistoryGrain records per-row-identity version history for this table.</summary>
    [Id(14)] public bool HistoryEnabled { get; set; }
    [Id(15)] public TableHistoryMode HistoryMode { get; set; } = TableHistoryMode.All;
    /// <summary>Version cap for LastN/FirstN modes.</summary>
    [Id(16)] public int HistoryLimit { get; set; } = 10;
    /// <summary>Output field (numeric or timestamp) MinBy/MaxBy ranks on. Required (and validated against
    /// OutputFields) when HistoryMode is MinBy or MaxBy.</summary>
    [Id(17)] public string? HistoryByField { get; set; }
    /// <summary>Retention time window in ms; versions older than (now - window) are pruned on append and
    /// on read. 0 = unbounded.</summary>
    [Id(18)] public long HistoryWindowMs { get; set; }

    // ------------------------------------------------------------------
    // Feature A: user-editable metadata. See SourceDefinition's doc comment for the same fields there.
    // ------------------------------------------------------------------

    [Id(19)] public List<string> Tags { get; set; } = [];
    [Id(20)] public Dictionary<string, string> Metadata { get; set; } = [];

    /// <summary>Plan 003 M2: opt-in partitioned execution. 1 (default) = the original single-grain
    /// TableGrain path, byte-for-byte unchanged (zero-risk default — see TableGrain's class comment on the
    /// Parallelism==1 fast path). 2..16 deploys the partitioned dataflow graph (TableIngestGrain +
    /// TableStageGrain × stages × partitions + TableOutputGrain) — see StreamsForge.Engine.Dataflow.TableDataflowPlan
    /// and TableGrain's Parallelism&gt;=2 coordinator-mode doc comment. Validated 1..16 by RegistryGrain;
    /// changing it restarts the table (same restart condition as a SQL/search-config change).</summary>
    [Id(21)] public int Parallelism { get; set; } = 1;

    /// <summary>Plan 008: how this table's materialized snapshot reaches durable storage.
    /// <see cref="TablePersistenceMode.Batched"/> (default) is the pre-008 behavior — a dirty flag plus a
    /// periodic flush that awaits the write inside the grain turn, so a flush stalls the table for as long
    /// as serializing the whole snapshot takes. The other two trade durability for that stall; see the enum.</summary>
    [Id(22)] public TablePersistenceMode Persistence { get; set; } = TablePersistenceMode.Batched;

    /// <summary>Flush interval in ms for <see cref="TablePersistenceMode.Batched"/> and
    /// <see cref="TablePersistenceMode.FireAndForget"/>. 0 = the 2000 ms default. Ignored for
    /// <see cref="TablePersistenceMode.MemoryOnly"/>. Changing it restarts the table.</summary>
    [Id(23)] public int FlushMs { get; set; }

    /// <summary>Plan 009 A2: journal length that triggers a compaction, for
    /// <see cref="TablePersistenceMode.Journaled"/> only. 0 = a sensible default. Too small and every
    /// flush degenerates into a full snapshot write (i.e. Batched with extra steps); too large and
    /// activation spends its time replaying.</summary>
    [Id(24)] public int JournalMaxEntries { get; set; }

    /// <summary>Plan 009 B2: where this table's deltas are republished. Empty = nowhere, the pre-009
    /// behavior. Delivery is fire-and-forget — see <see cref="SinkSpec"/>.</summary>
    [Id(25)] public List<SinkSpec> Sinks { get; set; } = [];

    /// <summary>Plan 015: who last changed this definition. CreatedBy has always been recorded; the
    /// counterpart was not, so "who broke prod" was unanswerable from the catalog alone. Set by
    /// CatalogRecordMerge's 4-arg overload on every update; empty on records last written before 015.
    ///
    /// <para><b>This was added at [Id(26)] and shipped that way, which was a COLLISION</b> —
    /// <see cref="RetentionMaxRows"/> has held 26 since plan 011 C2, further down the class where the
    /// "next free number" glance did not reach. Orleans' generated codec kept the first declaration and
    /// dropped this one, so the property round-tripped as empty through every grain call and every
    /// persisted snapshot: the field never worked on this flavour at all. Dapr, which serializes its
    /// state as JSON by property name, was unaffected — so the two flavours disagreed as well.
    /// Renumbered to 30 during plan 016 wave 0, before that wave added three more fields to this class.
    /// The cost of the renumber is stated and small: an UpdatedBy written between plan 015 wave 5 and
    /// this fix is not readable at 30 — but it was never readable at 26 either, so nothing that ever
    /// worked is lost. The permanent guard is ContractFieldNumberTests, which now fails the build on any
    /// duplicate rather than trusting the next person's glance.</para></summary>
    [Id(30)] public string UpdatedBy { get; set; } = "";

    // ------------------------------------------------------------------
    // Plan 011 C2: opt-in ROW RETENTION. Both default to 0 = OFF, and that default is the whole reason
    // this is safe to add to a frozen contract: an existing table keeps holding every row its SQL says it
    // should, exactly as before.
    //
    // READ THIS BEFORE TURNING EITHER ON. A table with retention is NOT the relation its SQL describes —
    // it is a BOUNDED VIEW of that relation. Rows that belong in the table by the SQL's own semantics are
    // deliberately dropped once a bound is exceeded, with a real retraction (so downstream tables, the
    // delta stream, SignalR, sinks, the search index and the row history all follow along and stay
    // consistent — the row genuinely LEAVES, it does not silently disappear). That is a change in results,
    // not just in memory use, and it is the price of bounding a table whose key space is unbounded
    // (a per-order GUID, a session id, a request id).
    //
    // Eviction is oldest-first by the row's EVENT timestamp (`_ts`), tie-broken deterministically, so
    // replaying the same input produces the same table — never wall-clock, never hash order. Supported
    // only on plan shapes whose whole per-row state is reclaimable (no joins, no set operations, no
    // derived sources, no GROUP BY/aggregates) and only on Parallelism == 1; RegistryGrain/CatalogStore
    // reject anything else up front rather than accepting a policy they could not honor. See
    // StreamsForge.Engine's TableRetentionPolicy / TablePlan.SupportsRetention for the full rationale.
    // ------------------------------------------------------------------

    /// <summary>Plan 011 C2: maximum rows this table retains; the oldest (by event timestamp) are evicted
    /// once the count exceeds it. 0 (default) = unbounded, i.e. the pre-011 behavior. See the block comment
    /// above — enabling this changes the table's results, on purpose.</summary>
    [Id(26)] public int RetentionMaxRows { get; set; }

    /// <summary>Plan 011 C2: maximum age of a retained row, in EVENT-time milliseconds measured back from
    /// the highest event timestamp this table has admitted (not from the wall clock — replay must be
    /// deterministic, and the honest consequence is that when the input stops, nothing further ages out).
    /// 0 (default) = unbounded. Composes with <see cref="RetentionMaxRows"/>: age is applied first, then
    /// the row-count bound.</summary>
    [Id(27)] public long RetentionTtlMs { get; set; }

    // ------------------------------------------------------------------
    // Plan 011 D1: opt-in KEY SHARDING. Empty (the default) = today's behavior, byte for byte — the same
    // opt-in discipline Parallelism established (DESIGN.md D9). Nothing about how the table is COMPUTED
    // changes when this is set: the shard tier is a second materialization fed by the table's own delta
    // stream (DESIGN.md D7, "the delta stream is the event log"), exactly like the row-history tier is.
    // The SQL path, the planner, the partitioned dataflow and every downstream table-over-table
    // subscriber are untouched.
    // ------------------------------------------------------------------

    /// <summary>Plan 011 D1: output column names this table's rows are sharded by. Empty (default) = not
    /// sharded. When non-empty, every delta the table emits is routed by these columns' values to a
    /// per-key <c>TableShardGrain</c> holding just that key's rows and just that key's version history —
    /// and, crucially, that grain does NOT pin itself alive: an idle key deactivates and its state lives
    /// on disk until the next lookup, which is the whole point (see orleans/DESIGN.md's "Sharded tables").
    ///
    /// Columns are EXPLICIT and validated against <see cref="OutputFields"/> at upsert. The textual
    /// GROUP BY / LATEST BY extraction the row-history tier falls back on
    /// (<c>TableGroupKeyExtractor.ExtractIdentityColumns</c>) is deliberately NOT used to pick one
    /// silently: it is best-effort matching, acceptable for "which versions belong together", not
    /// acceptable for "which grain owns this row".
    ///
    /// On a sharded table the per-key history REPLACES the single table-wide history grain (running both
    /// would double the memory this exists to save), and <see cref="SearchEnabled"/> is rejected outright
    /// (a table-wide inverted index keeps every row resident and defeats the point). Orleans-only: the
    /// Dapr flavor rejects a non-empty ShardBy at upsert, exactly as it already rejects
    /// <see cref="Parallelism"/> &gt; 1.</summary>
    [Id(28)] public List<string> ShardBy { get; set; } = [];

    // ------------------------------------------------------------------
    // Wishlist #18: row-identity KEY FIELDS on the wire. Server-owned (like OutputFields/StreamInputs/
    // TableInputs, next to which this is recomputed on every successful compile — see
    // StreamsForge.Host.Grains.TableKeyFields.Describe) — a client payload can never set it directly.
    // ------------------------------------------------------------------

    /// <summary>This table's logical row-identity key, for every delta-stream consumer that must
    /// supersede rows correctly instead of hand-maintaining its own key map (the problem wishlist #18
    /// exists to fix — as of this field, the console's <c>catalog.ts</c>, the Excel add-in's
    /// <c>KEY_FIELDS</c>, the Python client's key map, and a <c>/sql</c> editor's key box are all reading
    /// the SAME answer the engine already computes, instead of four hand-maintained copies of it).
    ///
    /// THE THREE WIRE STATES ARE NOT INTERCHANGEABLE — collapsing any two of them loses real information:
    /// <list type="bullet">
    /// <item><b>non-empty list</b> — the table's GROUP BY / LATEST BY identity, resolved to output column
    /// names in clause order. Supersede rows whose values agree on every one of these columns.</item>
    /// <item><b>empty list (<c>[]</c>)</b> — an UNKEYED GLOBAL AGGREGATE (e.g. <c>SELECT COUNT(*) FROM
    /// x</c> with no GROUP BY): the table always has exactly one row, so there is no key to compare —
    /// any new row simply replaces the one that came before it. This is "one global group", not "no
    /// identity".</item>
    /// <item><b>null</b> — WHOLE-ROW identity: no supersession key applies, and the row's entire content
    /// is what makes two rows the same or different. Covers two SQL shapes that behave identically for
    /// this purpose: a plain per-event passthrough (no GROUP BY/LATEST BY at all — the whole row always
    /// was the identity, and nothing is degraded), and a table that DOES declare a GROUP BY/LATEST BY key
    /// this extractor could not confidently map to an output column (the same fallback
    /// <c>TableRowIdentityWarning</c> reports on <c>GET /api/tables/{id}/metrics</c> when history or
    /// sharding is on) — <c>RowKeyCodec</c> keys THOSE rows by their whole content too, so null is the
    /// answer that matches actual dedup behavior, not merely the conservative one.</item>
    /// </list>
    ///
    /// Recomputed on every successful compile (create/update, and on seed) exactly like OutputFields is,
    /// so it is never stale relative to the table's current SQL and never client-writable — a compile
    /// failure resets it to null (whole-row), the same fail-safe default a never-compiled table starts
    /// with, so a wrong key can never silently collapse distinct rows.</summary>
    [Id(29)] public List<string>? KeyFields { get; set; }

    // --------------------------------------------------------------------------------------------
    // Plan 016 wave 0 — pre-built so wave 1's three concurrent agents never meet in this file.
    //
    // BOTH counters are REGISTRY-ASSIGNED and never client-settable: an incoming definition's values are
    // discarded exactly the way CreatedAtMs already is (CatalogRecordMerge.CarryServerOwnedFields), or a
    // caller could pin themselves to a revision they invented. Zero on records written before 016.
    // --------------------------------------------------------------------------------------------

    /// <summary>Monotonic, bumped whenever the stored definition actually changes — "changed" being
    /// canonical-JSON inequality, the same predicate ImportPlanner already uses to tell "skipped" from
    /// "updated", so a round-trip import that reports "skipped" provably does not bump this.</summary>
    [Id(31)] public long Revision { get; set; }

    /// <summary>Monotonic, bumped ONLY when the field shape changes — not when a knob does. That split is
    /// the entire reason a pin is useful: an eventsPerSecond edit must not invalidate a downstream
    /// dependant, and without two counters the choice is between pins that fire constantly and pins that
    /// never fire.</summary>
    [Id(32)] public long SchemaRevision { get; set; }

    /// <summary>Plan 016: what this entity was authored against, checked at exactly two moments — config
    /// import (against the post-import world, so mode=validate catches it before anything is applied) and
    /// start. Deliberately NOT re-checked continuously: a pin broken by an upstream change sets
    /// <see cref="StaleReason"/> and is badged, while the entity keeps running on its compiled plan, which
    /// is what it does today — only now visibly.
    ///
    /// <para>Pins live here and in config documents, never in SQL. <c>FROM trades@3</c> would touch the
    /// tokenizer, parser, AST, validator, planner, editor autocomplete, formatter and highlighter — the
    /// most expensive change available, in the one project all work serializes on — and it would have no
    /// coherent runtime meaning, because the engine executes against live streams with no versioned store
    /// to read revision 3 out of.</para></summary>
    [Id(33)] public List<EntityPin> DependsOn { get; set; } = [];

    /// <summary>Why this entity's pins no longer hold, or null when they do. Set by the upstream change
    /// that broke them; cleared when the pins are re-satisfied. A string rather than a flag because the
    /// only useful thing to render is WHICH dependency moved and from what — a boolean would send the
    /// operator to the logs to learn the one fact the badge exists to convey.</summary>
    [Id(34)] public string? StaleReason { get; set; }

    /// <summary>Plan 021 D5 — the environment this entity belongs to, empty for the default one. Written
    /// ONCE at creation from the request's environment and never edited afterwards: the name is in every
    /// runtime key this entity owns, so changing it would strand a grain, a state file and a stream the
    /// same way renaming a sharded table would.
    ///
    /// <para>It exists because the ambient (<c>EnvironmentAmbient</c>) answers "which catalog is this
    /// REQUEST talking to" and is empty everywhere else. Supervisors, the lifecycle orchestrator, connector
    /// drivers and stream bridges run on timers and subscriptions, outside any request — they read this
    /// field. Conflating the two is how background work silently operates on <c>default</c>.</para>
    ///
    /// <para>Deliberately NOT part of a config document (D8): a document carrying its environment would be
    /// deployable to exactly one place, which is the opposite of the point. The environment is a property
    /// of the import CALL. Config export therefore omits it, and import writes it from the target.</para></summary>
    [Id(35)] public string Environment { get; set; } = "";

    /// <summary>Table-over-pipeline: PIPELINE names this table reads directly, the third sibling of
    /// <see cref="StreamInputs"/> (raw sources) and <see cref="TableInputs"/> (upstream tables). Derived
    /// from the last successful compile, never user-editable; empty until the SQL compiles.
    ///
    /// <para>Why a separate list rather than more entries in <c>StreamInputs</c>: the two differ in the
    /// STREAM they are read from. A source input is subscribed on
    /// <c>(SourcesNamespace, qualified source name)</c> as <c>EventRecord</c>; a pipeline input is
    /// subscribed on <c>(OutputNamespace, qualified pipeline ID)</c> as <c>List&lt;ResultEnvelope&gt;</c>.
    /// The engine, which only ever sees a relation NAME and a schema, cannot tell them apart — the split
    /// happens here, in the registry, after the compile (see <c>RegistryGrain.ApplyCompileResult</c>),
    /// keyed off which catalog entity owns the name. That is also why a pipeline name may not collide
    /// with a source name and vice versa: one relation name must resolve to exactly one thing.</para>
    ///
    /// <para>Note the asymmetry with a table input: a pipeline has NO replay ring and no attach protocol,
    /// so a table attaching late to a pipeline starts empty and sees only rows published from that moment
    /// on. Same for a pipeline that stops or is deleted underneath a running table — the table stays
    /// Running and simply receives nothing more. Neither is refused.</para></summary>
    [Id(36)] public List<string> PipelineInputs { get; set; } = [];

    /// <summary>Plan 026 D5 (additive): where each SOURCE or PIPELINE input starts when this table
    /// (re)starts, keyed by input name — a producer position or an event time (see <see cref="ReplayFrom"/>).
    /// Applied at start only (fresh executor — see <see cref="PipelineDefinition.ReplayFrom"/> for why);
    /// changing it on a Running table restarts it. A TABLE input cannot be named here (its rows arrive
    /// through the backfill snapshot, plan 023's protocol; a position on a table input is wave 3 work) and
    /// validation refuses it with that reason. Dapr stores it and refuses to START with it set. Empty = off.</summary>
    [Id(37)] public Dictionary<string, ReplayFrom> ReplayFrom { get; set; } = [];
}

/// <summary>Plan 008: per-table durability policy. State is the materialized snapshot; the question is only
/// how it gets to storage, never how it is computed.
///
/// The cost being traded away is real and measurable: a flush serializes the ENTIRE snapshot into DTOs and
/// awaits the write **inside the grain turn**, so the stall grows with the row count and lands on the same
/// turn queue as incoming deltas.</summary>
[GenerateSerializer]
public enum TablePersistenceMode
{
    /// <summary>Dirty flag + periodic flush, awaited in the grain turn. Survives a restart, resuming from the
    /// last flush — up to one interval of deltas is lost. The pre-008 behavior and still the default.</summary>
    Batched,

    /// <summary>Same periodic flush, but the write is not awaited by the grain turn: the turn returns as soon
    /// as the snapshot is captured, and the write completes in the background (single-flight — a flush already
    /// in progress is not overlapped, the next tick is skipped instead). A crash loses whatever had not yet
    /// reached the disk, with no signal that it was lost.</summary>
    FireAndForget,

    /// <summary>Never written. The table lives entirely in the activation, so nothing touches storage on any
    /// path. A restart brings the table back **empty**, re-accumulating only from deltas that arrive after it
    /// — it does not replay history, so this suits tables that are naturally re-derivable or short-lived, and
    /// nothing else.</summary>
    MemoryOnly,

    /// <summary>Plan 009 A2. Same durability as <see cref="Batched"/>, but a flush writes only the rows that
    /// CHANGED since the last compaction (a separate, small journal state) instead of rewriting the whole
    /// snapshot — so write cost becomes O(changed) rather than O(|table|), which is what makes the flush
    /// interval stop being a latency knob on large tables. When the journal outgrows
    /// <see cref="TableDefinition.JournalMaxEntries"/> it is compacted: the full snapshot is written once
    /// and the journal truncated. Activation loads the snapshot and replays the journal over it, so the
    /// resumed state is identical to Batched's — the restart-resume limitation in TableGrain's class doc
    /// (output rows only, no operator internals) is unchanged and applies here too.</summary>
    Journaled,
}

/// <summary>Plan 003 M2: one partition's contribution to a partitioned table's aggregate
/// <see cref="TableMetrics"/> — additive detail, present only when Parallelism &gt;= 2 (see
/// TableMetrics.Partitions). StageId/Partition identify which TableStageGrain this is; the rest mirrors
/// TableMetrics' own per-activation counters at that grain.</summary>
[GenerateSerializer]
public sealed class TablePartitionMetrics
{
    [Id(0)] public int StageId { get; set; }
    [Id(1)] public int Partition { get; set; }
    [Id(2)] public long DeltasIn { get; set; }
    [Id(3)] public long DeltasOut { get; set; }
    [Id(4)] public long FrontierEpoch { get; set; } = -1;
    [Id(5)] public long LastUpdateMs { get; set; }
    /// <summary>Plan 003 M4: this stage's operator name (StreamsForge.Engine.Dataflow.TableStageKind, e.g.
    /// "Join"/"Reduce"/"FilterProject" — see StreamsForge.Engine.Dataflow.TableStageKindLabel), for the M5
    /// dataflow panel to render real operator names instead of bare stage ids. Additive; "" only if the
    /// producing grain somehow never learned its own stage descriptor (never happens in practice — see
    /// TableStageGrain.GetMetricsAsync).</summary>
    [Id(6)] public string Kind { get; set; } = "";
}

/// <summary>Serializable mirror of StreamsForge.Engine's TableDelta, for Orleans/SignalR transport: one Z-set
/// delta — a row entering (+1) or leaving (-1) a table's output.</summary>
[GenerateSerializer]
public sealed class TableDeltaDto
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; } = [];
    [Id(1)] public long Weight { get; set; }

    /// <summary>Plan 011 C2 — additive mirror of <c>StreamsForge.Engine.TableDelta.Retention</c> (default
    /// false, so every pre-011 producer and consumer is unchanged): true only for a retraction the table's
    /// ROW RETENTION policy caused, as opposed to one an upstream input caused. Consumers that do not care
    /// treat it as the ordinary retraction it also is; the one that does is the row-history grain/actor,
    /// which reclaims the evicted key's version list instead of counting one more retraction against a key
    /// that is never coming back (see TableDefinition.RetentionMaxRows' block comment).</summary>
    [Id(2)] public bool Evicted { get; set; }

    /// <summary>Wishlist #14 option (a) — additive (default -1, so every pre-existing producer/consumer of
    /// this DTO is unchanged): the epoch (<c>StreamsForge.Engine.TableExecutor.LastEpoch</c> at the moment
    /// of publish) the producing table admitted this delta under. Every element of one published batch
    /// shares the same value — the whole batch is one atomic admission (wishlist #15). Consumers that don't
    /// care ignore it; the one that does is a NEW table attaching to this one as a table input (see
    /// TableExecutor.LastEpoch's own doc comment for the full backfill-on-attach protocol this exists to
    /// make possible) — it drops any received delta whose Epoch is &lt;= the epoch its own attach snapshot
    /// was taken at, since that delta is already reflected in the snapshot it was seeded from.</summary>
    [Id(3)] public long Epoch { get; set; } = -1;
}

/// <summary>One row of a table's current consolidated Z-set snapshot (weight is always &gt; 0 in a
/// consolidated snapshot, but the DTO carries it through as-is for transport symmetry with TableDeltaDto).</summary>
[GenerateSerializer]
public sealed class TableRowDto
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; } = [];
    [Id(1)] public long Weight { get; set; }
}

[GenerateSerializer]
public sealed class TableMetrics
{
    [Id(0)] public string TableId { get; set; } = "";
    [Id(1)] public PipelineStatus Status { get; set; }
    [Id(2)] public long RowCount { get; set; }
    [Id(3)] public long DeltasIn { get; set; }
    [Id(4)] public long DeltasOut { get; set; }
    [Id(5)] public long LastUpdateMs { get; set; }
    /// <summary>True immediately after a restart-resume, until this table has rebuilt its state from live
    /// traffic — see TableGrain's rehydration-limitation comment.</summary>
    [Id(6)] public bool Rebuilding { get; set; }

    /// <summary>Plan 003 M2: per-partition detail, present (non-null) only for a Parallelism &gt;= 2 table —
    /// null/absent for every Parallelism==1 table, so this is additive-safe for existing consumers (REST
    /// JSON, gRPC, any client that ignores unknown fields).</summary>
    [Id(7)] public List<TablePartitionMetrics>? Partitions { get; set; }

    /// <summary>Plan 003 M3: distinct raw input names (stream sources / upstream tables) this table reads
    /// via a SHARED ArrangementGrain instead of a private per-table ingest — null/absent unless the table is
    /// Parallelism &gt;= 2 AND at least one of its join edges qualified as arrangeable (see
    /// StreamsForge.Engine.Dataflow.TableDataflowBuilder's arrangeability rule). Purely informational; does
    /// not affect Rebuilding (see that flag's own doc — an attached-but-still-rebuilding-from-checkpoint
    /// arrangement is folded into THIS table's own Rebuilding instead).</summary>
    [Id(8)] public List<string>? ArrangedInputs { get; set; }

    /// <summary>Plan 003 M4: the epoch this table's consolidated read-side snapshot (Snapshot/RowCount/
    /// search index — everything GetRowsAsync/SearchAsync serve) reflects, for a Parallelism &gt;= 2 table
    /// only — null/absent for every Parallelism==1 table (which has no partitioned frontier at all) AND
    /// for a Parallelism &gt;= 2 table that hasn't yet observed a full epoch from every terminal-stage
    /// partition (see TableGrain's OnOutputBatchAsync doc comment for exactly what this number means and
    /// the consistency guarantee it carries: the snapshot reflects ALL deltas whose epoch is &lt;= this
    /// value and NONE beyond it). Mirrors <see cref="Host.Api.TableRowsResponse.FrontierEpoch"/> (same
    /// value, exposed on the read path that actually needs it) — kept on both DTOs since GetRowsAsync
    /// callers (REST /rows, StreamsForge.Host.Api.TableRowsResponse.FrontierEpoch) shouldn't have to pay
    /// for a full GetMetricsAsync fan-out just to read it.</summary>
    [Id(9)] public long? SnapshotFrontierEpoch { get; set; }

    /// <summary>A plain-language warning that this table's per-row VERSION TRAIL is degraded, or null (the
    /// overwhelmingly common case) when it is not — the same kind of "this table is not in the state you
    /// think it is" condition <see cref="Rebuilding"/> reports, on the same object, for the same reason:
    /// the console is already looking here.
    ///
    /// It reports exactly one condition (composed by <c>TableRowIdentityWarning</c>, from the table's own
    /// definition): the SQL declares a GROUP BY / LATEST BY row identity whose keys the textual extractor
    /// could not map to output columns, so <c>RowKeyCodec.EncodeIdentity</c> keys rows by their WHOLE
    /// content and successive versions of one row never group into a trail — which is precisely what row
    /// history (and a shard's per-key trail) exists to accumulate. Reported only where it costs something,
    /// i.e. when HistoryEnabled or ShardBy is set; a table with no declared identity at all is never
    /// flagged, because the whole row genuinely IS its identity there and always was.
    ///
    /// DERIVED, NOT MEASURED — unlike every other field here. It is a pure function of the table's
    /// definition (Sql + HistoryEnabled + ShardBy), stamped onto the metrics by the shared
    /// <c>GET /api/tables/{id}/metrics</c> endpoint rather than counted by a grain/actor, so both runtime
    /// flavors report it from the identical code and it can never go stale relative to the SQL it
    /// describes. Additive and informational: nothing branches on it.</summary>
    [Id(10)] public string? RowIdentityWarning { get; set; }

    /// <summary>Plan 014: the output columns this table's declared row identity resolves to — the same
    /// derivation <see cref="RowIdentityWarning"/> reports the FAILURE of, reported here when it succeeds.
    /// Empty when the SQL declares no identity, or when it declares one the extractor could not map (in
    /// which case the warning above is non-null and a guess would be exactly the wrong thing to offer).
    ///
    /// It exists so the console can PREFILL a database sink's key columns in upsert mode. Deliberately a
    /// visible, editable suggestion rather than something the sink derives for itself: a sink client is
    /// handed only the entity name, so reaching back for its SQL would couple egress to the catalog — and
    /// where the extractor is uncertain, the operator is the one who can tell.
    ///
    /// DERIVED, NOT MEASURED, on the same terms and for the same reasons as the warning above.</summary>
    [Id(11)] public List<string> DeclaredKeyColumns { get; set; } = [];
}

/// <summary>
/// Plan 003 M3: request payload for <see cref="IArrangementGrain.AttachAsync"/> — everything a fresh
/// (refcount 0-&gt;1) arrangement activation needs to bootstrap itself (which raw input to subscribe to, the
/// raw field(s) forming its key, its partition identity) PLUS the routing info the arrangement needs to push
/// the atomic seed-then-live-deltas handshake directly to the attaching consumer's own ITableStageGrain (see
/// IArrangementGrain's class doc for why the arrangement — not the coordinator — must be the one to deliver
/// the snapshot, to avoid a snapshot/live-delta ordering race). Every field except <see cref="ConsumerId"/>/
/// <see cref="TargetGrainKey"/>/<see cref="TargetEdgeId"/> is redundant across repeated attaches to the SAME
/// arrangement (same keySpecHash ⇒ same InputName/KeyFields/KeySpec/PartitionCount/Partition by
/// construction) — only the FIRST attach (refcount 0-&gt;1) actually consumes them to activate; later attaches
/// trust the caller sent the same values (recompile-per-grain determinism — see GrainInterfaces.cs's M2
/// design note, which applies identically here) rather than re-validating.
/// </summary>
[GenerateSerializer]
public sealed class ArrangementAttachRequest
{
    /// <summary>Unique per (table, edge, partition) — e.g. "{tableName}:{edgeId}:{partition}". Used as the
    /// key DetachAsync later removes.</summary>
    [Id(0)] public string ConsumerId { get; set; } = "";
    /// <summary>The ITableStageGrain key ("{tableName}:{stageId}:{partition}") this arrangement partition
    /// pushes PushBatchAsync calls to.</summary>
    [Id(1)] public string TargetGrainKey { get; set; } = "";
    /// <summary>The EdgeId.Value the target's PushBatchAsync expects for this arrangement's contribution
    /// (the Left or Right join edge id on the CONSUMER's own dataflow plan).</summary>
    [Id(2)] public int TargetEdgeId { get; set; }
    /// <summary>Real stream source or upstream table name this arrangement indexes.</summary>
    [Id(3)] public string InputName { get; set; } = "";
    /// <summary>True if InputName is an upstream TABLE (subscribes to TableDeltaNamespace) rather than a raw
    /// stream SOURCE (SourcesNamespace).</summary>
    [Id(4)] public bool IsTableInput { get; set; }
    /// <summary>Raw field name(s), in order, forming this arrangement's key (see
    /// StreamsForge.Engine.Dataflow.ArrangementKeySpec).</summary>
    [Id(5)] public List<string> KeyFields { get; set; } = [];
    /// <summary>Human-readable canonical form of (KeyFields, PartitionCount) — carried for diagnostics/
    /// GetInfoAsync; the grain KEY itself uses ArrangementKeySpec.HashOf(KeySpec) instead.</summary>
    [Id(6)] public string KeySpec { get; set; } = "";
    [Id(7)] public int PartitionCount { get; set; }
    [Id(8)] public int Partition { get; set; }
}

/// <summary>Plan 003 M3: point-in-time view of one ArrangementGrain partition — backs GetInfoAsync and the
/// GET /api/meta/arrangements endpoint.</summary>
[GenerateSerializer]
public sealed class ArrangementInfo
{
    [Id(0)] public string InputName { get; set; } = "";
    [Id(1)] public string KeySpec { get; set; } = "";
    [Id(2)] public int Partition { get; set; }
    [Id(3)] public int PartitionCount { get; set; }
    [Id(4)] public long RowCount { get; set; }
    [Id(5)] public int ConsumerCount { get; set; }
    /// <summary>True from (re)activation off a persisted checkpoint until this partition has processed at
    /// least one live batch since — mirrors TableGrain's own restart-resume Rebuilding contract (see
    /// ArrangementGrain's class doc).</summary>
    [Id(6)] public bool Rebuilding { get; set; }
    /// <summary>Last epoch this partition stamped (-1 if it has never flushed).</summary>
    [Id(7)] public long Epoch { get; set; } = -1;
}

// ============================================================================
// Row history (Feature B) — see StreamsForge.Host.Grains.TableHistoryGrain.
// ============================================================================

/// <summary>One recorded ASSERTION version of a row-history entry: the row's content at a point in time,
/// plus a per-table monotonic sequence number (assigned from every delta the history grain observes,
/// assertion or retraction, so gaps between consecutive Seq values indicate retractions happened
/// in-between) for stable ordering.</summary>
// NOTE (005-W1): body-declared properties (plain `set`) instead of positional-record shorthand —
// see FieldDef's identical note above (ORLEANS0101 under cross-assembly codegen).
[GenerateSerializer]
public sealed record HistoryVersion
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; }
    [Id(1)] public long TsMs { get; set; }
    [Id(2)] public long Seq { get; set; }

    public HistoryVersion(Dictionary<string, object?> Row, long TsMs, long Seq)
    {
        this.Row = Row;
        this.TsMs = TsMs;
        this.Seq = Seq;
    }
}

/// <summary>Retention state for one row identity (see TableHistoryGrain / TableGroupKeyExtractor for how
/// the identity key is derived). Versions holds the retained ASSERTION history per the table's configured
/// HistoryMode; RetractionCount counts every retraction (weight &lt;= 0 delta) ever observed for this key —
/// retractions are not themselves stored as versions.</summary>
[GenerateSerializer]
public sealed class RowHistoryEntry
{
    [Id(0)] public List<HistoryVersion> Versions { get; set; } = [];
    [Id(1)] public long RetractionCount { get; set; }
}

/// <summary>Result of ITableHistoryGrain.GetHistoryAsync for one row identity.</summary>
[GenerateSerializer]
public sealed class TableHistoryQueryResult
{
    [Id(0)] public List<HistoryVersion> Versions { get; set; } = [];
    [Id(1)] public long RetractionCount { get; set; }
    [Id(2)] public TableHistoryMode Mode { get; set; }
    [Id(3)] public int TotalVersions { get; set; }
    /// <summary>False when the key has never been observed (as opposed to observed-but-empty).</summary>
    [Id(4)] public bool KeyFound { get; set; }
}

/// <summary>Result of ITableHistoryGrain.GetStatsAsync.</summary>
[GenerateSerializer]
public sealed class TableHistoryStats
{
    [Id(0)] public bool Enabled { get; set; }
    [Id(1)] public TableHistoryMode Mode { get; set; }
    [Id(2)] public int KeyCount { get; set; }
    [Id(3)] public long TotalVersions { get; set; }
}

/// <summary>
/// Plan 016 wave 0 — one entry in <see cref="PipelineDefinition.DependsOn"/> /
/// <see cref="TableDefinition.DependsOn"/>: "I was authored against THIS shape of THAT entity".
///
/// <para>Pinned on <see cref="SchemaRevision"/> and never on the plain revision, because the plain one
/// moves for every knob edit and a pin that fires on an unrelated change is a pin people learn to
/// ignore.</para>
/// </summary>
[GenerateSerializer]
public sealed class EntityPin
{
    /// <summary>"source" | "table". Pipelines are never depended upon — nothing reads a pipeline's
    /// output by name.</summary>
    [Id(0)] public string Kind { get; set; } = "";

    /// <summary>By NAME, not id, and that is forced rather than chosen: sources have no id at all, and a
    /// name is a stable key for exactly the entities whose runtime is name-keyed (sources cannot be
    /// renamed, tables only conditionally). Pipelines — the one freely renameable entity — are the one
    /// thing nothing pins.</summary>
    [Id(1)] public string Name { get; set; } = "";

    /// <summary>0 means "depends on it, pinned to nothing" — a declared edge with no compatibility
    /// claim, which is still worth recording because it is what import ordering needs.</summary>
    [Id(2)] public long SchemaRevision { get; set; }
}

[GenerateSerializer]
public sealed class UserRecord
{
    [Id(0)] public string Username { get; set; } = "";
    [Id(1)] public string DisplayName { get; set; } = "";
    /// <summary>"Admin" | "Editor" | "Viewer".</summary>
    [Id(2)] public string Role { get; set; } = "Viewer";
    [Id(3)] public string PasswordHash { get; set; } = "";
    [Id(4)] public string PasswordSalt { get; set; } = "";
    [Id(5)] public long CreatedAtMs { get; set; }

    // Plan 015. Only the OIDC seams land on the credential record; everything authorization reads
    // (Disabled, effective roles, grants) lives in AccessPolicyDocument.UserAccessEntry instead — see
    // AccessModels.cs's file header for why the resolver must never have a reason to read this type.
    /// <summary>OIDC seam: the IdP's stable subject for this user. Null for a local account.</summary>
    [Id(6)] public string? ExternalSubject { get; set; }
    /// <summary>OIDC seam: which IdP <see cref="ExternalSubject"/> belongs to. Null for a local account.</summary>
    [Id(7)] public string? IdentityProvider { get; set; }
}
