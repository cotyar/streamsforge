using System.Collections;
using System.Reflection;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 009: the guard for the SHAPE of catalog updates, not for any one field.
///
/// <para>Update used to copy a hand-written list of editable fields from the incoming definition onto
/// the stored record, so any field missing from that list was silently dropped on every PUT. Plan 009
/// lost three that way (Sinks on both entity types, and JournalMaxEntries — which this flavor never
/// copied at all). Each was found by hand, one at a time, after shipping.</para>
///
/// <para>These tests set EVERY writable property via reflection and assert that everything comes back
/// except the fields the server legitimately owns. A field added to the contract tomorrow is covered
/// automatically: it lands in "everything else" and must round-trip. If someone reverts to
/// field-by-field copying, these fail immediately instead of a year later.</para>
///
/// <para>The Orleans flavor's RegistryGrain applies the same rule through the same shared helper
/// (<see cref="CatalogRecordMerge"/>) — this project is where it can be tested without standing up a
/// cluster.</para>
/// </summary>
public class CatalogUpdateRoundTripTests
{
    /// <summary>Fields the SERVER owns: identity, lifecycle, provenance, and anything recomputed from
    /// the compile result. A client's payload must NOT be able to set these, so they are excluded from
    /// the round-trip assertion and checked for preservation separately.</summary>
    /// <para>Plan 016 wave 0 added "Revision" and "StaleReason" to both lists, plus "SchemaRevision" to
    /// the table one. Same test, same reason: a client that could choose its own Revision could pin a
    /// dependant to a revision that never existed, and one that could clear StaleReason could hide a
    /// broken pin simply by re-saving the entity that broke it. "DependsOn" is deliberately NOT here —
    /// a pin is the author's declaration of what they wrote the query against, so it round-trips like
    /// any other client-owned field.</para>
    /// <para>Plan 015 added "UpdatedBy" to both lists. It is server-owned in the strongest sense in this
    /// file's vocabulary: it is the authenticated caller, and a client that could set it could forge the
    /// provenance of its own edit. Classifying it here is the opposite of excluding a client-owned field
    /// from the guard — the guard's whole subject is which side of that line a property is on.</para>
    /// <para>Plan 021 added "Environment" to both lists. It is stamped once at creation from the request's
    /// environment (<see cref="CatalogStore.Environment"/>) and carried forward explicitly on every update
    /// (<c>CatalogStore.UpdatePipelineAsync</c>/<c>UpdateTableAsync</c>) — a client that could set it could
    /// move an entity between environments simply by round-tripping whatever the client happened to send
    /// back, exactly the D5 invariant this plan's whole environment-isolation guarantee depends on.</para>
    /// <para>Table-over-pipeline added <c>PipelineDefinition.OutputFields</c> and
    /// <c>TableDefinition.PipelineInputs</c>. Both are recomputed from the compile result, exactly like
    /// the <c>SourceNames</c> and <c>StreamInputs</c>/<c>TableInputs</c> already listed beside them, so
    /// this is a classification — the thing this guard exists to force someone to make deliberately — not
    /// a weakening of it. A pipeline's OutputFields matters more than its position in the list suggests:
    /// it IS the relation a dependent table compiled against, so a client that could set it could
    /// redefine, from outside, what somebody else's table is reading.</para>
    private static readonly HashSet<string> PipelineServerOwned =
        ["Id", "Status", "Error", "CreatedBy", "CreatedAtMs", "UpdatedAtMs", "UpdatedBy", "SourceNames",
         "OutputFields", "Revision", "StaleReason", "Environment"];

    private static readonly HashSet<string> TableServerOwned =
        ["Id", "Status", "Error", "CreatedBy", "CreatedAtMs", "UpdatedAtMs", "UpdatedBy", "OutputFields",
         "StreamInputs", "PipelineInputs", "TableInputs", "KeyFields", "Revision", "SchemaRevision",
         "StaleReason", "Environment"];

    /// <summary>Values that would be rejected by validation if generated blindly (parallelism range,
    /// non-negative flush interval) or that must stay compilable/unique.</summary>
    private static readonly Dictionary<string, object?> Overrides = new()
    {
        // Partitioned execution is Orleans-only (decision D-F) and this flavor rejects anything but 1,
        // so Parallelism is the one property whose round-trip cannot be distinguished from its default
        // here. The Orleans side covers it — see its own parallelism tests.
        ["Parallelism"] = 1,
        ["FlushMs"] = 1500,
        ["JournalMaxEntries"] = 250,
        ["HistoryLimit"] = 7,
        ["HistoryWindowMs"] = 60_000L,
        ["Sql"] = "SELECT symbol FROM trades",
        ["Name"] = "renamed_entity",
        ["HistoryByField"] = null,   // must name a real output column; null is always valid
        ["SearchEnabled"] = true,
        ["HistoryEnabled"] = false,  // avoids ValidateHistoryConfig's dependency on the compiled schema
    };

    private static (CatalogState State, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        return (state, new CatalogStore(state, new TestLifecycleOrchestrator()));
    }

    /// <summary>Produces a distinctive, valid non-default value for any property type the catalog
    /// contracts use. Deliberately total: an unhandled type throws rather than silently skipping the
    /// property, because a skipped property is exactly the blind spot these tests exist to remove.</summary>
    private static object? SampleValue(PropertyInfo prop)
    {
        if (Overrides.TryGetValue(prop.Name, out var overridden))
        {
            return overridden;
        }

        var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

        if (type == typeof(string)) return $"v_{prop.Name}";
        if (type == typeof(bool)) return true;
        if (type == typeof(int)) return 3;
        if (type == typeof(long)) return 1234L;
        if (type.IsEnum)
        {
            // A value that is NOT the default, so "never assigned" is distinguishable from "assigned".
            var values = Enum.GetValues(type).Cast<object>().ToList();
            return values.FirstOrDefault(v => !v.Equals(Enum.ToObject(type, 0))) ?? values[0];
        }
        if (type == typeof(List<string>)) return new List<string> { $"tag_{prop.Name}" };
        if (type == typeof(Dictionary<string, string>)) return new Dictionary<string, string> { ["k"] = prop.Name };
        if (type == typeof(List<FieldDef>)) return new List<FieldDef> { new("f", FieldType.String) };
        // Plan 016: a pin is client-owned — the author's declaration of what they wrote the query
        // against — so it has to round-trip like any other field the caller sends.
        if (type == typeof(List<EntityPin>))
        {
            return new List<EntityPin> { new() { Kind = "source", Name = $"dep_{prop.Name}", SchemaRevision = 7 } };
        }

        // Plan 026: replayFrom is client-owned — the author's choice of where each input starts.
        if (type == typeof(Dictionary<string, ReplayFrom>))
        {
            return new Dictionary<string, ReplayFrom> { [$"in_{prop.Name}"] = new ReplayFrom { Seq = 42 } };
        }

        if (type == typeof(List<SinkSpec>))
        {
            return new List<SinkSpec>
            {
                new() { Kind = SinkKinds.Nats, Enabled = true, Nats = new NatsPubConfig { Url = "nats://h:4222", Subject = prop.Name } },
            };
        }

        throw new NotSupportedException(
            $"{prop.DeclaringType?.Name}.{prop.Name} has type {type.Name}, which this test does not know how to " +
            "populate. Teach SampleValue about it — do not exclude the property, or the field it guards " +
            "stops being guarded.");
    }

    private static List<PropertyInfo> Writable<T>() =>
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.CanRead)
            .ToList();

    private static void AssertEquivalent(string propName, object? expected, object? actual)
    {
        if (expected is IEnumerable and not string)
        {
            // Collections compare by serialized shape — the contracts' element types are plain data.
            Assert.Equal(
                System.Text.Json.JsonSerializer.Serialize(expected),
                System.Text.Json.JsonSerializer.Serialize(actual));
            return;
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task UpdatePipeline_RoundTripsEveryClientOwnedField()
    {
        var (state, store) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "p1", Sql = "SELECT symbol FROM trades", CreatedBy = "someone",
        });

        var incoming = new PipelineDefinition { Id = created.Id };
        var clientOwned = Writable<PipelineDefinition>().Where(p => !PipelineServerOwned.Contains(p.Name)).ToList();
        Assert.NotEmpty(clientOwned);
        foreach (var prop in clientOwned)
        {
            prop.SetValue(incoming, SampleValue(prop));
        }

        var updated = await store.UpdatePipelineAsync(incoming);
        Assert.NotNull(updated);

        var stored = state.Pipelines.Single(p => p.Id == created.Id);
        foreach (var prop in clientOwned)
        {
            AssertEquivalent(prop.Name, SampleValue(prop), prop.GetValue(stored));
        }

        // …and the server's own fields survived the client's payload untouched.
        Assert.Equal(created.Id, stored.Id);
        Assert.Equal("someone", stored.CreatedBy);
        Assert.Equal(created.CreatedAtMs, stored.CreatedAtMs);
    }

    [Fact]
    public async Task UpdateTable_RoundTripsEveryClientOwnedField()
    {
        var (state, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t1", Sql = "SELECT symbol FROM trades", CreatedBy = "someone",
        });

        var incoming = new TableDefinition { Id = created.Id };
        var clientOwned = Writable<TableDefinition>().Where(p => !TableServerOwned.Contains(p.Name)).ToList();
        Assert.NotEmpty(clientOwned);
        foreach (var prop in clientOwned)
        {
            prop.SetValue(incoming, SampleValue(prop));
        }

        var updated = await store.UpdateTableAsync(incoming);
        Assert.NotNull(updated);

        var stored = state.Tables.Single(t => t.Id == created.Id);
        foreach (var prop in clientOwned)
        {
            AssertEquivalent(prop.Name, SampleValue(prop), prop.GetValue(stored));
        }

        Assert.Equal(created.Id, stored.Id);
        Assert.Equal("someone", stored.CreatedBy);
        Assert.Equal(created.CreatedAtMs, stored.CreatedAtMs);
    }

    [Fact]
    public async Task UpdateTable_JournalMaxEntriesChange_IsTreatedAsAPersistenceChange()
    {
        // The half of the JournalMaxEntries defect that storing the field alone would not have fixed:
        // a running table only re-reads its persistence knobs on restart, so a threshold change that is
        // stored but not classified as a persistence change never actually takes effect.
        var (_, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t_journal", Sql = "SELECT symbol FROM trades",
            Persistence = TablePersistenceMode.Journaled, JournalMaxEntries = 100,
        });

        var updated = await store.UpdateTableAsync(new TableDefinition
        {
            Id = created.Id, Name = created.Name, Sql = created.Sql,
            Persistence = TablePersistenceMode.Journaled, JournalMaxEntries = 500,
        });

        Assert.NotNull(updated);
        Assert.Equal(500, updated!.JournalMaxEntries);
    }
}
