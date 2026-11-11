using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Json;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 011 wave D1 — the cold-read path, which is the ONE path a sharded table exercises constantly and
/// an in-memory test cluster never exercises at all.
///
/// A shard's whole purpose is to deactivate and come back from storage. In production that storage is
/// <c>JsonFileGrainStorage</c>, and System.Text.Json materializes every value of an untyped
/// <c>Dictionary&lt;string, object?&gt;</c> as a boxed <see cref="JsonElement"/>. A cluster test using
/// Orleans' default memory storage round-tripped through a different serializer that preserved the CLR
/// types, so it was structurally incapable of catching this — which is exactly what happened: the wave's
/// live check hit an HTTP 500 on the first lookup of a deactivated shard, because Orleans has no
/// serializer for JsonElement and the grain was handing one straight back in a response. (Since
/// 2026-11-11 the cluster tests use <c>AddJsonMemoryGrainStorage</c>, which serializes with
/// JsonFileGrainStorage's own System.Text.Json options, so they now see the same JsonElement values.)
///
/// These tests pin both halves of the fix: that the hazard is real (a round-trip really does produce
/// JsonElement), and that normalizing restores values a live shard would be indistinguishable from —
/// including the canonical row key, because a reactivated shard whose keys differed from the router's
/// would silently split one row into two ledger entries rather than fail loudly.
/// </summary>
public class ShardStateRehydrationTests
{
    private static TableShardGrainState SampleState() => new()
    {
        TableName = "instruments",
        ShardKey = "instrumentS:XS123",
        Config = new TableShardConfig
        {
            TableName = "instruments",
            ShardBy = ["instrument"],
            IdentityColumns = ["instrument", "leg"],
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
        },
        Rows = new Dictionary<string, TableRowDto>(StringComparer.Ordinal)
        {
            ["k1"] = new()
            {
                Row = new Dictionary<string, object?>
                {
                    ["instrument"] = "XS123",
                    ["leg"] = 2L,
                    ["stage"] = "Settled",
                    ["notional"] = 1234.5,
                    ["active"] = true,
                    ["note"] = null,
                },
                Weight = 1,
            },
        },
        History = new Dictionary<string, RowHistoryEntry>(StringComparer.Ordinal)
        {
            ["instrumentS:XS123legL:2"] = new()
            {
                Versions =
                [
                    new HistoryVersion(new Dictionary<string, object?> { ["instrument"] = "XS123", ["leg"] = 2L, ["stage"] = "New" }, 100, 1),
                    new HistoryVersion(new Dictionary<string, object?> { ["instrument"] = "XS123", ["leg"] = 2L, ["stage"] = "Settled" }, 200, 2),
                ],
                RetractionCount = 1,
            },
        },
        AppliedSeq = 42,
        Seq = 2,
        DeltasApplied = 3,
    };

    /// <summary>Round-trips through the same serializer <c>JsonFileGrainStorage</c> uses.</summary>
    private static TableShardGrainState RoundTrip(TableShardGrainState state) =>
        JsonSerializer.Deserialize<TableShardGrainState>(JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = false }))!;

    [Fact]
    public void PersistedShardState_ComesBackWithJsonElementValues_TheHazardIsReal()
    {
        var revived = RoundTrip(SampleState());

        // Not an assertion about what we WANT — an assertion about what the storage layer actually does,
        // so that if System.Text.Json's behavior ever changes the normalization below is revisited rather
        // than silently kept as dead code.
        Assert.All(revived.Rows["k1"].Row.Values.Where(v => v is not null), v => Assert.IsType<JsonElement>(v));
    }

    [Fact]
    public void NormalizingARehydratedRow_RestoresTheSameClrShapesALiveShardHolds()
    {
        var revived = RoundTrip(SampleState());
        var row = new Dictionary<string, object?>(revived.Rows["k1"].Row);
        JsonValueNormalizer.NormalizeInPlace(row);

        Assert.Equal("XS123", row["instrument"]);
        Assert.Equal(2L, row["leg"]);          // long, NOT double — the shard key and the ledger key both depend on it
        Assert.Equal("Settled", row["stage"]);
        Assert.Equal(1234.5, row["notional"]);
        Assert.Equal(true, row["active"]);
        Assert.Null(row["note"]);
        Assert.DoesNotContain(row.Values, v => v is JsonElement);
    }

    [Fact]
    public void NormalizedRehydratedRow_ProducesTheSameCanonicalAndShardKeysAsTheLiveRow()
    {
        var live = SampleState().Rows["k1"].Row;
        var revived = new Dictionary<string, object?>(RoundTrip(SampleState()).Rows["k1"].Row);
        JsonValueNormalizer.NormalizeInPlace(revived);

        // If these diverged, a reactivated shard would file the same row under a different ledger key and
        // consolidation would double it — silently, with no error anywhere.
        Assert.Equal(TableShardKeys.CanonicalRowKey(live), TableShardKeys.CanonicalRowKey(revived));
        Assert.Equal(
            TableShardKeys.EncodeShardKey(live, ["instrument"]),
            TableShardKeys.EncodeShardKey(revived, ["instrument"]));
        Assert.Equal(
            RowKeyCodec.EncodeIdentity(live, ["instrument", "leg"]),
            RowKeyCodec.EncodeIdentity(revived, ["instrument", "leg"]));
    }

    [Fact]
    public void NormalizingARehydratedHistoryVersion_RestoresItsRowToo()
    {
        var revived = RoundTrip(SampleState());
        var entry = revived.History["instrumentS:XS123legL:2"];
        Assert.Equal(2, entry.Versions.Count);
        Assert.Equal(1, entry.RetractionCount);

        var versionRow = new Dictionary<string, object?>(entry.Versions[1].Row);
        JsonValueNormalizer.NormalizeInPlace(versionRow);
        Assert.Equal("Settled", versionRow["stage"]);
        Assert.Equal(2L, versionRow["leg"]);
        Assert.DoesNotContain(versionRow.Values, v => v is JsonElement);
    }

    [Fact]
    public void ScalarBookkeepingSurvivesTheRoundTripUntouched()
    {
        var revived = RoundTrip(SampleState());
        Assert.Equal("instruments", revived.TableName);
        Assert.Equal("instrumentS:XS123", revived.ShardKey);
        Assert.Equal(42, revived.AppliedSeq);
        Assert.Equal(2, revived.Seq);
        Assert.Equal(3, revived.DeltasApplied);
        Assert.NotNull(revived.Config);
        Assert.Equal(new[] { "instrument" }, revived.Config!.ShardBy.ToArray());
        Assert.Equal(new[] { "instrument", "leg" }, revived.Config.IdentityColumns!.ToArray());
        Assert.True(revived.Config.HistoryEnabled);
    }
}
