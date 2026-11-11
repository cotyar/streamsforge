using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 002 L2 — UNNEST(expr) AS alias: both grammar forms (comma-form sugar and JOIN/CROSS JOIN form),
/// element binding (a Json-kind pseudo-column addressed only via '->'/'->>'), and both runtime modes
/// (pipeline 1-to-N row expansion, table Z-set-linear weight passthrough).
/// </summary>
public class UnnestTests
{
    private static List<object?> Legs(params Dictionary<string, object?>[] legs) => legs.Cast<object?>().ToList();

    private static Dictionary<string, object?> Leg(string ccy, double notional) =>
        new() { ["ccy"] = ccy, ["notional"] = notional };

    // ------------------------------------------------------------------
    // Parser
    // ------------------------------------------------------------------

    [Fact]
    public void CommaFormUnnestCompiles()
    {
        var r = Compile("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void JoinFormUnnestCompiles()
    {
        var r = Compile("SELECT l ->> 'ccy' AS ccy FROM structures s JOIN UNNEST(s.legs) AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void CrossJoinFormUnnestCompiles()
    {
        var r = Compile("SELECT l ->> 'ccy' AS ccy FROM structures s CROSS JOIN UNNEST(s.legs) AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void MultipleUnnestsInCommaFormBothReferencingTheBaseSourceCompile()
    {
        var r = Compile("SELECT l1 ->> 'ccy' AS c1, l2 ->> 'ccy' AS c2 FROM structures s, UNNEST(s.legs) AS l1, UNNEST(s.legs) AS l2", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void UnnestWithoutAliasIsRejected()
    {
        var r = Compile("SELECT * FROM structures s, UNNEST(s.legs)", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("UNNEST requires an alias"));
    }

    [Fact]
    public void UnnestAsFirstFromItemIsRejected()
    {
        var r = Compile("SELECT * FROM UNNEST(s.legs) AS l", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("UNNEST cannot be the first FROM item"));
    }

    [Fact]
    public void LeftJoinUnnestIsRejected()
    {
        var r = Compile("SELECT * FROM structures s LEFT JOIN UNNEST(s.legs) AS l", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("no LEFT UNNEST"));
    }

    [Fact]
    public void UnnestOfAnotherUnnestAliasIsRejectedWithDiagnostic()
    {
        var r = Compile("SELECT * FROM structures s, UNNEST(s.legs) AS l1, UNNEST(l1) AS l2", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("may not reference another UNNEST alias"));
    }

    // ------------------------------------------------------------------
    // Validator
    // ------------------------------------------------------------------

    [Fact]
    public void UnknownColumnInUnnestExprIsError()
    {
        var r = Compile("SELECT * FROM structures s, UNNEST(s.bogus) AS l", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown column 'bogus'"));
    }

    [Fact]
    public void UnnestOfNonJsonExprIsError()
    {
        var r = Compile("SELECT * FROM structures s, UNNEST(s.trade_id) AS l", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("UNNEST argument must be a JSON value"));
    }

    [Fact]
    public void UnnestAliasDotFieldGivesTailoredDiagnostic()
    {
        var r = Compile("SELECT l.ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("has no field 'ccy'") && d.Message.Contains("->>"));
    }

    [Fact]
    public void BareUnnestAliasInSelectEmitsElementAsJsonKind()
    {
        var r = Compile("SELECT l FROM structures s, UNNEST(s.legs) AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Json, r.OutputSchema!.Fields["l"]);
    }

    [Fact]
    public void QualifiedStarOverUnnestAliasExpands()
    {
        var r = Compile("SELECT l.* FROM structures s, UNNEST(s.legs) AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Single(r.OutputSchema!.Fields);
    }

    [Fact]
    public void JsonPathExprAsUnnestArgumentCompiles()
    {
        var r = Compile("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.payload -> 'legs') AS l", Structures);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    // ------------------------------------------------------------------
    // Executor — pipelines
    // ------------------------------------------------------------------

    [Fact]
    public void UnnestEmitsOneRowPerElementInArrayOrder()
    {
        var exec = CompileAndCreate("SELECT s.trade_id AS id, l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        var legs = Legs(Leg("USD", 100), Leg("EUR", 200), Leg("GBP", 300));
        var results = exec.OnEvent("structures", Evt(1000, "structures", ("trade_id", "T1"), ("legs", legs), ("payload", null)));

        Assert.Equal(3, results.Count);
        Assert.Equal(["USD", "EUR", "GBP"], results.Select(r => r["ccy"]));
        Assert.True(results.All(r => (string)r["id"]! == "T1"));
    }

    [Fact]
    public void UnnestOfNullArrayEmitsZeroRows()
    {
        var exec = CompileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        var results = exec.OnEvent("structures", Evt(1000, "structures", ("trade_id", "T1"), ("legs", null), ("payload", null)));
        Assert.Empty(results);
    }

    [Fact]
    public void UnnestOfEmptyArrayEmitsZeroRows()
    {
        var exec = CompileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        var results = exec.OnEvent("structures", Evt(1000, "structures", ("trade_id", "T1"), ("legs", Legs()), ("payload", null)));
        Assert.Empty(results);
    }

    [Fact]
    public void UnnestOfNonArrayValueEmitsZeroRows()
    {
        var exec = CompileAndCreate("SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        // legs holds a scalar, not a JSON array/list — still a Json-kind field statically, but the runtime
        // value isn't a List<object?>, so it's the dynamic "non-array value" case documented for UNNEST.
        var results = exec.OnEvent("structures", Evt(1000, "structures", ("trade_id", "T1"), ("legs", 42L), ("payload", null)));
        Assert.Empty(results);
    }

    [Fact]
    public void JoinAfterUnnestWithWithinMatchesMarketData()
    {
        var sql = "SELECT l ->> 'ccy' AS ccy, q.bid AS bid " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "JOIN quotes q WITHIN 5 SECONDS ON (l ->> 'ccy') = q.symbol";
        var exec = CompileAndCreate(sql, Structures, Quotes);

        exec.OnEvent("quotes", Evt(900, "quotes", ("symbol", "USD"), ("bid", 1.1), ("ask", 1.2)));
        var results = exec.OnEvent("structures", Evt(1000, "structures",
            ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100), Leg("EUR", 200))), ("payload", null)));

        var match = Assert.Single(results);
        Assert.Equal("USD", match["ccy"]);
        Assert.Equal(1.1, match["bid"]);
    }

    [Fact]
    public void AcceptancePlan002L2_SumOverLegsPerCcyTumblingWindow()
    {
        // Plan 002 L2 acceptance query, adapted to this dialect's pinned rules:
        //  - GROUP BY must repeat the exact grouped EXPRESSION (this dialect has no group-by-alias sugar —
        //    see ExecutorWindowAndAggregateTests's existing `GROUP BY payload -> 'user' ->> 'tier'` pattern)
        //  - numeric aggregation over a JSON leaf uses '->' (raw numeric node), NOT '->>' (stringifies —
        //    see SumOfArrowArrowTextDoesNotAccumulateNumerically below for why)
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS total_notional " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "GROUP BY l ->> 'ccy' " +
                  "WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Structures);

        exec.OnEvent("structures", Evt(1000, "structures",
            ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100), Leg("EUR", 200), Leg("USD", 50))), ("payload", null)));
        var results = exec.AdvanceWatermark(10_000);

        Assert.Contains(results, r => (string)r["ccy"]! == "USD" && (double)r["total_notional"]! == 150.0);
        Assert.Contains(results, r => (string)r["ccy"]! == "EUR" && (double)r["total_notional"]! == 200.0);
    }

    [Fact]
    public void SumOfArrowArrowTextDoesNotAccumulateNumerically()
    {
        // Documents the pinned coercion rule: '->>' always returns TEXT (see JsonAccessExpr.ReturnText),
        // and SumAggregator/AvgAggregator only recognize `long`/`double` inputs (see Aggregators.cs) — a
        // TEXT value silently contributes nothing. Use '->' for numeric aggregation instead (see the
        // acceptance test above). With nothing numeric contributed, SUM is NULL (PostgreSQL's SUM over no
        // values; this asserted 0 before 2026-11-11, when SUM was aligned with PostgreSQL at the user's request).
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l ->> 'notional') AS total_notional " +
                  "FROM structures s, UNNEST(s.legs) AS l " +
                  "GROUP BY l ->> 'ccy' " +
                  "WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Structures);

        exec.OnEvent("structures", Evt(1000, "structures",
            ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100))), ("payload", null)));
        var results = exec.AdvanceWatermark(10_000);

        var row = Assert.Single(results, r => (string)r["ccy"]! == "USD");
        Assert.Null(row["total_notional"]); // NOT 100 — the documented '->>' aggregation trap
    }

    // ------------------------------------------------------------------
    // Executor — tables
    // ------------------------------------------------------------------

    [Fact]
    public void TableUnnestRetractionRetractsAllElementRows()
    {
        var exec = CompileTableAndCreate("SELECT s.trade_id AS id, l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l", Structures);
        var row = Evt(0, "structures", ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100), Leg("EUR", 200), Leg("GBP", 300))), ("payload", null));

        var asserted = exec.OnStreamEvent("structures", row);
        Assert.Equal(3, asserted.Count);
        Assert.True(asserted.All(d => d.Weight == 1));

        var retracted = exec.OnTableDelta("structures", new TableDelta(row, -1));
        Assert.Equal(3, retracted.Count);
        Assert.True(retracted.All(d => d.Weight == -1));
        Assert.Equal(
            asserted.Select(d => d.Row["ccy"]).OrderBy(x => x),
            retracted.Select(d => d.Row["ccy"]).OrderBy(x => x));

        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void TableUnnestUnderGroupByEmitsRetractAssertPerAggregateChange()
    {
        var sql = "SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS total FROM structures s, UNNEST(s.legs) AS l GROUP BY l ->> 'ccy'";
        var exec = CompileTableAndCreate(sql, Structures);

        var row1 = Evt(0, "structures", ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100))), ("payload", null));
        var first = exec.OnStreamEvent("structures", row1);
        var assertion = Assert.Single(first);
        Assert.Equal(1, assertion.Weight);
        Assert.Equal(100.0, assertion.Row["total"]);

        var row2 = Evt(0, "structures", ("trade_id", "T2"), ("legs", Legs(Leg("USD", 50))), ("payload", null));
        var second = exec.OnStreamEvent("structures", row2);
        Assert.Equal(2, second.Count);
        Assert.Equal(-1, second[0].Weight);
        Assert.Equal(100.0, second[0].Row["total"]);
        Assert.Equal(1, second[1].Weight);
        Assert.Equal(150.0, second[1].Row["total"]);
    }

    [Fact]
    public void NestedUnnestInsideDerivedTableWorks()
    {
        var sql = "SELECT d.ccy FROM (SELECT l ->> 'ccy' AS ccy FROM structures s, UNNEST(s.legs) AS l) d";
        var exec = CompileTableAndCreate(sql, Structures);

        var row = Evt(0, "structures", ("trade_id", "T1"), ("legs", Legs(Leg("USD", 100), Leg("EUR", 200))), ("payload", null));
        var deltas = exec.OnStreamEvent("structures", row);

        Assert.Equal(2, deltas.Count);
        Assert.Equal(["EUR", "USD"], deltas.Select(d => d.Row["ccy"]).OrderBy(x => x));
    }
}
