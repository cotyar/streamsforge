using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using StreamsForge.Abstractions;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.RepresentationModel;

namespace StreamsForge.AppCore.Config;

/// <summary>
/// Plan 006 (D-I): the shared plumbing behind <see cref="ConfigSerializer"/>/
/// <see cref="ConfigComposer"/>/<see cref="ImportPlanner"/>/<see cref="SecretsMasker"/> — text
/// (JSON or YAML) &lt;-&gt; <see cref="JsonNode"/> &lt;-&gt; <see cref="ConfigDocument"/>, plus the
/// canonical-node builder that both the byte-equality writer and the entity-diff comparisons
/// (<see cref="ImportPlanner"/>) key off of. Internal — not part of the pinned public surface.
///
/// <para><b>One JSON model, two encodings (D-A/D-I):</b> YAML input is bridged into the exact same
/// <see cref="JsonNode"/> shape JSON input produces (scalars resolved per YAML 1.1 core-schema-ish
/// rules: quoted scalars are always strings; unquoted "null"/"~"/"" → null, "true"/"false" (any
/// case) → bool, integer/float-looking tokens → number, else string) — so every downstream
/// consumer (document mapping, diagnostics, composition) has exactly one code path
/// (<see cref="NodeToDocument"/>) regardless of which encoding the caller used.</para>
///
/// <para><b>Canonical omission rule (byte-equality contract, tested in ConfigSerializerTests):</b>
/// a property is omitted from the canonical node when its value is JSON <c>null</c>, an empty
/// array, or an empty object — recursively, bottom-up (a nested object that becomes empty AFTER
/// its own children are pruned is itself omitted from its parent). The <b>only</b> exception is
/// <c>kind</c> on a source entity, which is ALSO omitted when it equals the literal string
/// <see cref="SourceKinds.Generator"/> (the default kind — not otherwise null/empty, so this needs
/// its own rule). <c>version</c> and <c>running</c> are never actually touched by the null/empty
/// rule in practice (int/bool values are never null nor an empty collection) — they are
/// unconditionally emitted, matching the "kept always" contract by construction, not by a special
/// case. Numeric/bool fields whose value is their CLR zero (e.g. <c>eventsPerSecond: 0</c>,
/// <c>historyLimit: 0</c>) are intentionally NOT omitted — the rule is exactly "null or empty
/// collection", nothing broader, so it is unambiguous and requires no per-property default table.
/// </para>
/// </summary>
internal static class ConfigJsonMapper
{
    /// <summary>Options used for every model &lt;-&gt; JsonNode conversion in this file: camelCase
    /// property names, enum values as their exact C# member name (case-insensitive on read —
    /// verified: System.Text.Json's constructor-parameter binding, used for <see cref="FieldDef"/>
    /// which has no parameterless constructor, matches case-insensitively regardless of this
    /// setting; <see cref="JsonStringEnumConverter"/> likewise accepts either case on read but
    /// always writes the exact member name).</summary>
    internal static readonly JsonSerializerOptions ModelOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private static readonly JsonSerializerOptions CanonicalWriteOptions = new()
    {
        WriteIndented = true,
        IndentSize = 2,
    };

    // ------------------------------------------------------------------
    // Text -> JsonNode (sniff JSON vs YAML; D-I "one STJ path").
    // ------------------------------------------------------------------

    /// <summary>Sniffs the first non-whitespace character: <c>{</c> means JSON, anything else
    /// (including an all-whitespace/empty string) means YAML. Returns (null, diagnostics) only on
    /// an actual syntax error; an empty/all-whitespace document is a legitimate empty config and
    /// yields an empty <see cref="JsonObject"/> with no diagnostics. diagnostics is always non-null
    /// (possibly empty).</summary>
    internal static (JsonNode? Node, List<string> Diagnostics) TextToNode(string text)
    {
        var diagnostics = new List<string>();
        var firstChar = FirstNonWhitespaceChar(text);

        if (firstChar == '{')
        {
            try
            {
                return (JsonNode.Parse(text), diagnostics);
            }
            catch (JsonException ex)
            {
                diagnostics.Add($"invalid JSON: {ex.Message}");
                return (null, diagnostics);
            }
        }

        try
        {
            // An all-whitespace/empty document is a legitimate empty config ({}), not an error —
            // YamlStream.Load produces zero Documents for it, which YamlToJsonNode reports as null;
            // treat that specifically as "{}" so Parse below yields a valid, empty ConfigDocument
            // instead of failing. A YAML document that's present but not a mapping (e.g. a bare
            // scalar or a list) still flows through as-is and is rejected downstream by
            // NodeToDocument's "root must be a JSON object" check.
            var node = YamlToJsonNode(text) ?? new JsonObject();
            return (node, diagnostics);
        }
        catch (YamlException ex)
        {
            diagnostics.Add($"invalid YAML: {ex.Message}");
            return (null, diagnostics);
        }
    }

    private static char? FirstNonWhitespaceChar(string text)
    {
        foreach (var c in text)
        {
            if (!char.IsWhiteSpace(c))
            {
                return c;
            }
        }

        return null;
    }

    private static JsonNode? YamlToJsonNode(string text)
    {
        var stream = new YamlStream();
        stream.Load(new StringReader(text));
        return stream.Documents.Count == 0 ? null : ConvertYamlNode(stream.Documents[0].RootNode);
    }

    private static JsonNode? ConvertYamlNode(YamlNode node) => node switch
    {
        YamlScalarNode scalar => ConvertYamlScalar(scalar),
        YamlSequenceNode seq => new JsonArray([.. seq.Children.Select(ConvertYamlNode)]),
        YamlMappingNode map => ConvertYamlMapping(map),
        _ => null,
    };

    private static JsonObject ConvertYamlMapping(YamlMappingNode map)
    {
        var obj = new JsonObject();
        foreach (var (keyNode, valueNode) in map.Children)
        {
            var key = keyNode is YamlScalarNode ks ? ks.Value ?? "" : keyNode.ToString();
            obj[key] = ConvertYamlNode(valueNode);
        }

        return obj;
    }

    /// <summary>YAML 1.1-core-schema-ish scalar resolution: a quoted scalar (single/double-quoted,
    /// literal <c>|</c>, folded <c>&gt;</c>) is always a string regardless of its content; a plain
    /// (unquoted) scalar is resolved as null/bool/number/string in that order. Documented, closed —
    /// no timestamps, no octal/hex literals, no YAML 1.1 sexagesimal numbers.</summary>
    private static JsonNode? ConvertYamlScalar(YamlScalarNode scalar)
    {
        var value = scalar.Value ?? "";
        if (scalar.Style != YamlDotNet.Core.ScalarStyle.Plain)
        {
            return JsonValue.Create(value);
        }

        switch (value)
        {
            case "" or "~" or "null" or "Null" or "NULL":
                return null;
            case "true" or "True" or "TRUE":
                return JsonValue.Create(true);
            case "false" or "False" or "FALSE":
                return JsonValue.Create(false);
        }

        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
        {
            return JsonValue.Create(l);
        }

        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
        {
            return JsonValue.Create(d);
        }

        return JsonValue.Create(value);
    }

    // ------------------------------------------------------------------
    // JsonNode -> ConfigDocument.
    // ------------------------------------------------------------------

    /// <summary>Maps a parsed <see cref="JsonNode"/> to a <see cref="ConfigDocument"/>. Fatal
    /// structural errors (root not an object; <c>version</c> present but not an integer or not 1;
    /// a JSON type error anywhere STJ's constructor/property binding trips over, e.g. "sources" not
    /// an array) return (null, diagnostics). Non-fatal per-entity errors (an entity missing/blank
    /// "name") drop just that entity and add a diagnostic, keeping the rest of the document.</summary>
    internal static (ConfigDocument? Doc, List<string> Diagnostics) NodeToDocument(JsonNode node)
    {
        var diagnostics = new List<string>();

        if (node is not JsonObject root)
        {
            diagnostics.Add("root must be a JSON object");
            return (null, diagnostics);
        }

        if (root.TryGetPropertyValue("version", out var versionNode) && versionNode is not null)
        {
            if (versionNode is JsonValue v && TryGetInt32(v, out var version))
            {
                if (version != 1)
                {
                    diagnostics.Add($"unsupported version: {version} (expected 1)");
                    return (null, diagnostics);
                }
            }
            else
            {
                diagnostics.Add("'version' must be an integer");
                return (null, diagnostics);
            }
        }

        ConfigDocument? doc;
        try
        {
            doc = root.Deserialize<ConfigDocument>(ModelOptions);
        }
        catch (JsonException ex)
        {
            diagnostics.Add($"structurally invalid document: {ex.Message}");
            return (null, diagnostics);
        }

        doc ??= new ConfigDocument();
        NormalizeCollections(doc);

        PruneUnnamed(doc.Sources, s => s.Name, "sources", diagnostics);
        PruneUnnamed(doc.Pipelines, p => p.Name, "pipelines", diagnostics);
        PruneUnnamed(doc.Tables, t => t.Name, "tables", diagnostics);

        return (doc, diagnostics);
    }

    /// <summary>STJ sets a property to a literal JSON <c>null</c> when present (e.g. an
    /// include-composed document explicitly clearing an optional field — D-I "explicit null clears
    /// an optional field"); for the handful of properties whose CLR type is a non-nullable
    /// collection with a non-null default (<c>= []</c>), that would otherwise leave a null
    /// reference where callers expect an empty collection. Recursively restores the empty-collection
    /// default; leaves genuinely optional nullable scalars (HistoryByField, DedupKeyField, ...) and
    /// FieldDef.Children (null IS a valid "no children" state there) untouched.</summary>
    private static void NormalizeCollections(ConfigDocument doc)
    {
        doc.Include ??= [];
        doc.Sources ??= [];
        doc.Pipelines ??= [];
        doc.Tables ??= [];
        doc.Requires ??= []; // Plan 016 wave 4 — same "explicit null -> empty default" rule as Include/etc.
        foreach (var r in doc.Requires)
        {
            // A per-item explicit JSON null for these string properties survives STJ's constructor
            // binding as an actual null despite the non-nullable CLR type and the `= ""`/`= "*"`
            // initializers (they only apply when the property is genuinely absent) — normalize the
            // same way PruneUnnamed's callers already assume string properties are never null.
            r.Kind ??= "";
            r.Version ??= "*";
        }

        foreach (var s in doc.Sources)
        {
            s.Fields ??= [];
            s.Tags ??= [];
            s.Metadata ??= [];
            if (s.Connector is { } connector)
            {
                if (connector.Url is { } url)
                {
                    url.Headers ??= [];
                }

                if (connector.Mapping is { } mapping)
                {
                    mapping.Fields ??= [];
                }
            }
        }

        foreach (var p in doc.Pipelines)
        {
            p.Tags ??= [];
            p.Metadata ??= [];
            p.Sinks ??= [];
            p.DependsOn ??= []; // Plan 016 wave 3-B — same "explicit null -> empty default" rule as Sinks.
            p.ReplayFrom ??= []; // Plan 026 wave 3 — same rule, one level deeper (a Dictionary, not a List).
        }

        foreach (var t in doc.Tables)
        {
            t.Tags ??= [];
            t.Metadata ??= [];
            t.Sinks ??= [];
            t.DependsOn ??= []; // Plan 016 wave 3-B — same "explicit null -> empty default" rule as Sinks.
            t.ReplayFrom ??= []; // Plan 026 wave 3 — same rule, one level deeper (a Dictionary, not a List).
        }
    }

    private static void PruneUnnamed<T>(List<T> list, Func<T, string?> nameOf, string label, List<string> diagnostics)
    {
        for (var i = list.Count - 1; i >= 0; i--)
        {
            if (!string.IsNullOrWhiteSpace(nameOf(list[i])))
            {
                continue;
            }

            diagnostics.Add($"{label}[{i}]: missing name — entry skipped");
            list.RemoveAt(i);
        }
    }

    // ------------------------------------------------------------------
    // ConfigDocument -> canonical JsonNode (D-I byte-equality contract).
    // ------------------------------------------------------------------

    /// <summary>Builds the canonical root node: <c>version</c> always first and always present,
    /// then <c>include</c>/<c>sources</c>/<c>pipelines</c>/<c>tables</c> (each omitted entirely
    /// when empty), entities within each array sorted by Name (ordinal).</summary>
    internal static JsonObject DocumentToNode(ConfigDocument doc)
    {
        var root = new JsonObject { ["version"] = doc.Version };

        if (doc.Include.Count > 0)
        {
            root["include"] = new JsonArray([.. doc.Include.Select(s => (JsonNode?)JsonValue.Create(s))]);
        }

        // Plan 016 wave 3: emitted ONLY when the document actually declares one, so every export
        // written before this field is still byte-identical — absent already means "compatible", which
        // is the gated default. See ConfigDocument.SchemaPolicy for why the default is the strict one.
        if (!string.IsNullOrWhiteSpace(doc.SchemaPolicy))
        {
            root["schemaPolicy"] = doc.SchemaPolicy;
        }

        // Plan 016 wave 4: same "emitted only when the document actually declares one" rule as
        // schemaPolicy just above — every export written before this field is byte-identical. Ordered
        // by Kind (ordinal) for the same stable-export reason entity arrays are ordered by Name.
        var requires = doc.Requires.OrderBy(r => r.Kind, StringComparer.Ordinal).Select(RequirementNode).ToArray();
        if (requires.Length > 0)
        {
            root["requires"] = new JsonArray([.. requires.Select(n => (JsonNode?)n)]);
        }

        var sources = doc.Sources.OrderBy(s => s.Name, StringComparer.Ordinal).Select(SourceNode).ToArray();
        if (sources.Length > 0)
        {
            root["sources"] = new JsonArray([.. sources.Select(n => (JsonNode?)n)]);
        }

        var pipelines = doc.Pipelines.OrderBy(p => p.Name, StringComparer.Ordinal).Select(PipelineNode).ToArray();
        if (pipelines.Length > 0)
        {
            root["pipelines"] = new JsonArray([.. pipelines.Select(n => (JsonNode?)n)]);
        }

        var tables = doc.Tables.OrderBy(t => t.Name, StringComparer.Ordinal).Select(TableNode).ToArray();
        if (tables.Length > 0)
        {
            root["tables"] = new JsonArray([.. tables.Select(n => (JsonNode?)n)]);
        }

        return root;
    }

    /// <summary>Canonical node for one <see cref="SourceDefinition"/> — declaration order (Name,
    /// Description, Fields, GeneratorProfile, EventsPerSecond, Enabled, Tags, Metadata, Kind,
    /// Connector), empties/nulls pruned, <c>kind</c> additionally omitted when it's the default
    /// "generator". Reused by <see cref="ImportPlanner"/> for entity-equality comparison.</summary>
    internal static JsonObject SourceNode(SourceDefinition s)
    {
        var node = JsonSerializer.SerializeToNode(s, ModelOptions)!.AsObject();
        PruneEmpty(node);
        if (node.TryGetPropertyValue("kind", out var kindNode) &&
            kindNode is JsonValue kv && kv.TryGetValue<string>(out var kind) && kind == SourceKinds.Generator)
        {
            node.Remove("kind");
        }

        // Plan 021 D8: the config document stays environment-free. A source is the one entity type this
        // file serializes WHOLE (pipelines and tables go through the trimmed ConfigPipeline/ConfigTable
        // projections, which simply never carry the field), so its environment has to be dropped here or
        // an export from `staging` would re-import into `prod` carrying "staging" with it — the opposite
        // of the promotion this plan exists to make possible. The environment is a property of the import
        // CALL, never of the document. An empty string is not pruned by PruneEmpty (the rule is null /
        // empty array / empty object, deliberately), so this is unconditional rather than a default-value
        // special case. ImportPlanner compares entities through this same node, which is what makes an
        // otherwise-identical entity plan as "skipped" across environments instead of churning Revision.
        node.Remove("environment");

        return node;
    }

    /// <summary>Canonical node for one <see cref="ConfigPluginRequirement"/> — declaration order
    /// (kind, version), empties/nulls pruned (in practice neither field is ever null/empty after
    /// <see cref="NormalizeCollections"/>, so this mirrors the other *Node helpers for consistency
    /// rather than because pruning does anything here).</summary>
    internal static JsonObject RequirementNode(ConfigPluginRequirement r)
    {
        var node = JsonSerializer.SerializeToNode(r, ModelOptions)!.AsObject();
        PruneEmpty(node);
        return node;
    }

    /// <summary>Canonical node for one <see cref="ConfigPipeline"/> — declaration order, empties/
    /// nulls pruned, <c>running</c> always kept (never null/empty so never pruned in practice —
    /// documented per the class-level contract, not a special case here).</summary>
    internal static JsonObject PipelineNode(ConfigPipeline p)
    {
        var node = JsonSerializer.SerializeToNode(p, ModelOptions)!.AsObject();
        PruneEmpty(node);
        return node;
    }

    /// <summary>Canonical node for one <see cref="ConfigTable"/> — same treatment as
    /// <see cref="PipelineNode"/> plus the table-only fields, all in declaration order.</summary>
    internal static JsonObject TableNode(ConfigTable t)
    {
        var node = JsonSerializer.SerializeToNode(t, ModelOptions)!.AsObject();
        PruneEmpty(node);
        return node;
    }

    /// <summary>Recursively removes any property whose value is JSON null, an empty array, or an
    /// empty object — bottom-up, so a nested object that becomes empty after ITS children are
    /// pruned is itself omitted from its parent (e.g. an all-null ScheduleSpec disappears from its
    /// containing ConnectorConfig, which then disappears from the source entity if nothing else in
    /// it survived pruning either).</summary>
    private static void PruneEmpty(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                var snapshot = obj.ToList();
                var toRemove = new List<string>();
                foreach (var (key, value) in snapshot)
                {
                    PruneEmpty(value);
                    if (IsNullOrEmpty(obj[key]))
                    {
                        toRemove.Add(key);
                    }
                }

                foreach (var key in toRemove)
                {
                    obj.Remove(key);
                }

                break;
            }

            case JsonArray arr:
            {
                foreach (var item in arr)
                {
                    PruneEmpty(item);
                }

                break;
            }
        }
    }

    /// <summary>Reads a JSON number as an <see cref="int"/> regardless of which numeric CLR type
    /// backs the <see cref="JsonValue"/> — <see cref="JsonValue.TryGetValue{TValue}"/> requires an
    /// EXACT backing-type match for values not backed by a <see cref="JsonElement"/> (e.g. a
    /// YAML-origin value created via <c>JsonValue.Create(1L)</c> fails <c>TryGetValue&lt;int&gt;</c>
    /// even though 1L trivially fits in an int) — see <see cref="EmitYamlScalar"/>'s identical
    /// concern on the write side. Shared by <see cref="NodeToDocument"/> and
    /// <see cref="ConfigComposer"/>'s version check.</summary>
    internal static bool TryGetInt32(JsonValue value, out int result)
    {
        if (value.TryGetValue(out result))
        {
            return true;
        }

        if (value.TryGetValue<long>(out var l) && l is >= int.MinValue and <= int.MaxValue)
        {
            result = (int)l;
            return true;
        }

        if (value.TryGetValue<double>(out var d) && d == Math.Floor(d) && d is >= int.MinValue and <= int.MaxValue)
        {
            result = (int)d;
            return true;
        }

        result = 0;
        return false;
    }

    private static bool IsNullOrEmpty(JsonNode? node) => node switch
    {
        null => true,
        JsonArray a => a.Count == 0,
        JsonObject o => o.Count == 0,
        _ => false,
    };

    // ------------------------------------------------------------------
    // Canonical text rendering.
    // ------------------------------------------------------------------

    internal static string ToCanonicalJsonText(JsonObject node) => node.ToJsonString(CanonicalWriteOptions);

    /// <summary>Renders a (canonical) node as YAML: 2-space indent, block-style sequences indented
    /// under their key. Every string scalar is double-quoted UNCONDITIONALLY (never plain) so a
    /// string value that happens to look like a bool/number/null (e.g. a source literally named
    /// "true") round-trips as a string — verified empirically: YamlDotNet's high-level
    /// <c>Serializer</c> does NOT do this (it emits bare/plain scalars for such strings, which then
    /// misparse on the way back in), so this uses the low-level <c>Emitter</c> event stream instead,
    /// choosing the scalar style explicitly per JsonValue kind. No byte-equality contract for YAML
    /// (only JSON has one, per D-I) — this only needs to be correct and stably ordered, which it is
    /// by construction (fed the already name-sorted, already-pruned canonical node).</summary>
    internal static string NodeToYaml(JsonObject node)
    {
        using var writer = new StringWriter();
        var settings = new EmitterSettings().WithBestIndent(2).WithIndentedSequences();
        var emitter = new Emitter(writer, settings);
        emitter.Emit(new StreamStart());
        emitter.Emit(new DocumentStart(null, null, true));
        EmitYamlNode(emitter, node);
        emitter.Emit(new DocumentEnd(true));
        emitter.Emit(new StreamEnd());
        return writer.ToString();
    }

    private static void EmitYamlNode(IEmitter emitter, JsonNode? node)
    {
        switch (node)
        {
            case null:
                emitter.Emit(new Scalar(null, null, "null", YamlDotNet.Core.ScalarStyle.Plain, true, false));
                break;

            case JsonObject obj:
                emitter.Emit(new MappingStart(null, null, true, MappingStyle.Block));
                foreach (var (key, value) in obj)
                {
                    emitter.Emit(new Scalar(null, null, key, YamlDotNet.Core.ScalarStyle.Plain, true, false));
                    EmitYamlNode(emitter, value);
                }

                emitter.Emit(new MappingEnd());
                break;

            case JsonArray arr:
                emitter.Emit(new SequenceStart(null, null, true, SequenceStyle.Block));
                foreach (var item in arr)
                {
                    EmitYamlNode(emitter, item);
                }

                emitter.Emit(new SequenceEnd());
                break;

            case JsonValue val:
                EmitYamlScalar(emitter, val);
                break;
        }
    }

    private static void EmitYamlScalar(IEmitter emitter, JsonValue val)
    {
        // JsonValue.TryGetValue<T> requires T to match the ORIGINAL backing CLR type (no int-&gt;long
        // widening) — SerializeToNode over the model preserves each property's actual numeric type
        // (int for HistoryLimit/Parallelism/Version, long for HistoryWindowMs, double for
        // EventsPerSecond), so every numeric width used anywhere in the model needs its own branch.
        if (val.TryGetValue<bool>(out var b))
        {
            emitter.Emit(new Scalar(null, null, b ? "true" : "false", YamlDotNet.Core.ScalarStyle.Plain, true, false));
        }
        else if (val.TryGetValue<int>(out var i))
        {
            emitter.Emit(new Scalar(null, null, i.ToString(CultureInfo.InvariantCulture), YamlDotNet.Core.ScalarStyle.Plain, true, false));
        }
        else if (val.TryGetValue<long>(out var l))
        {
            emitter.Emit(new Scalar(null, null, l.ToString(CultureInfo.InvariantCulture), YamlDotNet.Core.ScalarStyle.Plain, true, false));
        }
        else if (val.TryGetValue<double>(out var d))
        {
            emitter.Emit(new Scalar(null, null, d.ToString(CultureInfo.InvariantCulture), YamlDotNet.Core.ScalarStyle.Plain, true, false));
        }
        else
        {
            var s = val.GetValue<string>();
            emitter.Emit(new Scalar(null, null, s, YamlDotNet.Core.ScalarStyle.DoubleQuoted, false, true));
        }
    }

    // ------------------------------------------------------------------
    // Generic deep clone (JSON round-trip through the same options everything else uses).
    // ------------------------------------------------------------------

    internal static T DeepCloneModel<T>(T value) =>
        JsonSerializer.SerializeToNode(value, ModelOptions)!.Deserialize<T>(ModelOptions)!;
}
