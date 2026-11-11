using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 004 N2 (`[NOT] IN`/`[NOT] EXISTS` → semi/anti join), N3 (uncorrelated scalar subqueries), and N4
/// (single-level equality-correlated scalar subqueries, decorrelated to a GROUP-BY-k join) — grammar,
/// validator diagnostics, table-mode retraction-correct executor semantics, and pipeline-mode rolling-
/// snapshot semantics. Builds directly on N1's derived-table/CTE machinery (NestedQueriesN1Tests.cs) — a
/// subquery here is always compiled the same way an N1 derived table is (BuildCompiledPlan/
/// BuildCompiledTablePlan recursion), just wired as a synthesized join stage instead of a plain FROM/JOIN
/// alias. See Sql/Validator.cs's SubqueryPredicateInfo/ScalarSubqueryInfo doc comments and
/// Planning/Planner.cs's RewriteWhereForSubqueryPredicates/RewriteScalarSubqueries for the plan-time
/// rewrite, and Runtime/Ops/TableSemiAntiOp.cs / Runtime/Ops/PipelineSubqueryOp.cs for the runtime
/// semantics pinned by the tests below.
/// </summary>
public class NestedQueriesN2N3N4Tests
{
    // ------------------------------------------------------------------
    // Parser / basic compile shape
    // ------------------------------------------------------------------

    [Fact]
    public void InSubqueryParsesAndCompilesInTableMode()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void NotInSubqueryParsesAndCompiles()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void ExistsParsesAndCompiles()
    {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void NotExistsParsesAndCompiles()
    {
        var sql = "SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void ScalarSubqueryInSelectListParsesAndCompiles()
    {
        var sql = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";
        var r = CompileTable(sql, [Trades]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["rel"]);
    }

    [Fact]
    public void ScalarSubqueryInWhereParsesAndCompiles()
    {
        var sql = "SELECT symbol FROM trades WHERE price > (SELECT AVG(price) FROM trades)";
        var r = CompileTable(sql, [Trades]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void DoublyParenthesizedScalarSubqueryCompiles()
    {
        var sql = "SELECT symbol FROM trades WHERE price > ((SELECT AVG(price) FROM trades))";
        var r = CompileTable(sql, [Trades]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    // ------------------------------------------------------------------
    // Validator diagnostics
    // ------------------------------------------------------------------

    [Fact]
    public void InSubqueryInSelectListIsRejected()
    {
        var sql = "SELECT symbol IN (SELECT symbol FROM ref) AS flag FROM trades";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("only allowed in WHERE"));
    }

    [Fact]
    public void InSubqueryNestedInsideOrIsRejected()
    {
        var sql = "SELECT symbol FROM trades WHERE active = true OR symbol IN (SELECT symbol FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("top-level AND-connected"));
    }

    [Fact]
    public void ExistsInOnClauseIsRejected()
    {
        var sql = "SELECT t.symbol FROM trades t JOIN ref r ON EXISTS (SELECT 1 FROM ref) AND t.symbol = r.symbol";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("only allowed in WHERE"));
    }

    [Fact]
    public void NonWindowedInSubqueryInPipelineIsRejected()
    {
        var sql = "SELECT symbol, COUNT(*) AS c FROM trades WHERE symbol IN (SELECT symbol FROM ref) GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades, Ref);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("must be windowed"));
    }

    [Fact]
    public void NonAggregateScalarSubqueryIsRejected()
    {
        var sql = "SELECT symbol, (SELECT symbol FROM ref) AS x FROM trades";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("single-row aggregate query"));
    }

    [Fact]
    public void ScalarSubqueryWithOwnGroupByIsRejected()
    {
        var sql = "SELECT symbol, (SELECT AVG(price) FROM trades GROUP BY symbol) AS x FROM trades";
        var r = CompileTable(sql, [Trades]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("may not have its own GROUP BY"));
    }

    [Fact]
    public void CorrelationBeyondEqualityIsRejected()
    {
        var sql = "SELECT t.symbol, (SELECT AVG(r.price) FROM trades r WHERE r.symbol > t.symbol) AS x FROM trades t";
        var r = CompileTable(sql, [Trades]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("rewrite as a JOIN"));
    }

    [Fact]
    public void UnknownCorrelatedColumnIsRejected()
    {
        var sql = "SELECT t.symbol, (SELECT AVG(r.price) FROM trades r WHERE r.symbol = t.bogus) AS x FROM trades t";
        var r = CompileTable(sql, [Trades]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown column 'bogus' on 't'"));
    }

    [Fact]
    public void ScalarSubqueryInGroupedSelectListIsRejected()
    {
        var sql = "SELECT symbol, SUM(qty), (SELECT AVG(price) FROM trades) AS avgall FROM trades GROUP BY symbol";
        var r = CompileTable(sql, [Trades]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("grouped/windowed/aggregated"));
    }

    [Fact]
    public void InSubqueryMustSelectExactlyOneColumnIsEnforced()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol, tag FROM ref)";
        var r = CompileTable(sql, [Trades], [Ref]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("must select exactly one column"));
    }

    // ------------------------------------------------------------------
    // Table mode — semi/anti join (N2) retraction-correct executor semantics
    // ------------------------------------------------------------------

    [Fact]
    public void SemiJoinPassesRowsWhoseKeyIsPresentInB()
    {
        var sql = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "x")), 1));

        var passed = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var row = Assert.Single(passed);
        Assert.Equal(1, row.Weight);
        Assert.Equal("AAPL", row.Row["symbol"]);

        var filtered = exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 1L), ("active", true)));
        Assert.Empty(filtered);
    }

    [Fact]
    public void SemiJoinBSideDuplicatesDoNotFanOutARows()
    {
        // Plan 004 N2: "duplicates in B must not duplicate A's rows — presence, not join fan-out."
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "x")), 1));
        var second = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "y")), 1));
        Assert.Empty(second); // presence didn't flip (already present) — no re-emission from the 2nd B row either

        var passed = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var row = Assert.Single(passed); // exactly ONE row despite B holding 2 AAPL contributions
        Assert.Equal(1, row.Weight);
    }

    [Fact]
    public void SemiJoinBRetractionCascadesFlippingARows()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "x")), 1));
        var asserted = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        Assert.Single(asserted, d => d.Weight == 1);

        // Retracting B's last row for key "AAPL" must retract every currently-matching A row, even though
        // no NEW trades event arrived.
        var cascaded = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "x")), -1));
        var retraction = Assert.Single(cascaded);
        Assert.Equal(-1, retraction.Weight);
        Assert.Equal("AAPL", retraction.Row["symbol"]);
    }

    [Fact]
    public void NotInWithANullInTheSubqueryFiltersNonMatchingRows()
    {
        // PostgreSQL three-valued logic: `'MSFT' NOT IN ('AAPL', NULL)` is NULL, so the row is filtered.
        // (Changed 2026-11-11 at the user's request: this test used to pin plan 004's deviation, which
        // ignored NULLs in the subquery and let MSFT through. See PostgresNullSemanticsTests.)
        var sql = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "x")), 1));
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", null), ("tag", "y")), 1));

        var result = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 1L), ("active", true)));
        Assert.Empty(result);
    }

    [Fact]
    public void ExistsRetroactivelyAssertsPreviouslyFilteredRowsWhenBBecomesNonEmpty()
    {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        var beforeB = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        Assert.Empty(beforeB); // ref is empty -> EXISTS is false -> filtered

        var afterB = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "ANYTHING"), ("tag", "x")), 1));
        var row = Assert.Single(afterB);
        Assert.Equal(1, row.Weight);
        Assert.Equal("AAPL", row.Row["symbol"]);
    }

    [Fact]
    public void NotExistsPassesWhenBIsEmptyAndRetractsOnceBGetsARow()
    {
        var sql = "SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        var passed = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        Assert.Single(passed, d => d.Weight == 1);

        var retracted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "ANYTHING"), ("tag", "x")), 1));
        var row = Assert.Single(retracted);
        Assert.Equal(-1, row.Weight);
        Assert.Equal("AAPL", row.Row["symbol"]);
    }

    // ------------------------------------------------------------------
    // Table mode — scalar subquery (N3) retraction-correct executor semantics
    // ------------------------------------------------------------------

    [Fact]
    public void ScalarSubqueryChangeCascadesRetractAssertOfEveryAffectedRow()
    {
        var sql = "SELECT q.symbol, q.bid - (SELECT AVG(price) FROM trades) AS rel FROM quotes q";
        var exec = CompileTableAndCreate(sql, [Trades, Quotes]);

        // No trades yet -> the scalar subquery has no rows -> the quotes row finds no match (plain
        // singleton equi-join, no null-padding) and is dropped for now.
        var beforeAgg = exec.OnStreamEvent("quotes", Evt(1000, "quotes", ("symbol", "AAPL"), ("bid", 100.0), ("ask", 101.0)));
        Assert.Empty(beforeAgg);

        // First trades row establishes the aggregate's first value (AVG=100) — this retroactively matches
        // the already-indexed quotes row.
        var firstAgg = exec.OnStreamEvent("trades", Evt(1100, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var firstMatch = Assert.Single(firstAgg);
        Assert.Equal(1, firstMatch.Weight);
        Assert.Equal(0.0, firstMatch.Row["rel"]);

        // A second trades row changes the aggregate (AVG becomes 150) -> every affected output row must
        // retract its old value and assert the new one.
        var changed = exec.OnStreamEvent("trades", Evt(1200, "trades", ("symbol", "MSFT"), ("price", 200.0), ("qty", 1L), ("active", true)));
        Assert.Equal(2, changed.Count);
        var retraction = Assert.Single(changed, d => d.Weight == -1);
        Assert.Equal(0.0, retraction.Row["rel"]);
        var assertion = Assert.Single(changed, d => d.Weight == 1);
        Assert.Equal(-50.0, assertion.Row["rel"]); // 100 - 150
    }

    // ------------------------------------------------------------------
    // Table mode — N4 decorrelation equivalence
    // ------------------------------------------------------------------

    [Fact]
    public void N4DecorrelatedScalarSubqueryMatchesHandWrittenEquivalentJoin()
    {
        const string n4Sql = "SELECT t.symbol, t.price, (SELECT AVG(r.price) FROM trades r WHERE r.symbol = t.symbol) AS avgbysym FROM trades t";
        const string handWrittenSql = "WITH bysym AS (SELECT symbol, AVG(price) AS avgbysym FROM trades GROUP BY symbol) " +
                                       "SELECT t.symbol, t.price, b.avgbysym FROM trades t JOIN bysym b ON t.symbol = b.symbol";

        var n4Result = CompileTable(n4Sql, [Trades]);
        Assert.True(n4Result.Ok, string.Join(";", n4Result.Diagnostics));
        var handWrittenResult = CompileTable(handWrittenSql, [Trades]);
        Assert.True(handWrittenResult.Ok, string.Join(";", handWrittenResult.Diagnostics));

        var n4Exec = n4Result.Plan!.CreateExecutor();
        var handWrittenExec = handWrittenResult.Plan!.CreateExecutor();

        var events = new[]
        {
            Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)),
            Evt(2000, "trades", ("symbol", "AAPL"), ("price", 200.0), ("qty", 1L), ("active", true)),
            Evt(3000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 1L), ("active", true)),
        };
        foreach (var evt in events)
        {
            n4Exec.OnStreamEvent("trades", evt);
            handWrittenExec.OnStreamEvent("trades", evt);
        }

        // _source differs by construction (N4's decorrelated join uses an internal-only synthetic alias
        // that never appears in the hand-written version's SourceLabel) — compare everything else.
        string CanonSnapshot(TableExecutor e) => string.Join("|", e.Snapshot().Values.Select(v =>
        {
            var withoutSource = new EventRecord(v.Row);
            withoutSource.Remove(EventRecord.SourceField);
            return $"{v.Weight}:{Runtime.JsonText.SerializeCanonicalRow(withoutSource)}";
        }).OrderBy(s => s, StringComparer.Ordinal));

        Assert.Equal(3, n4Exec.Snapshot().Count);
        Assert.Equal(CanonSnapshot(handWrittenExec), CanonSnapshot(n4Exec));
    }

    // ------------------------------------------------------------------
    // Pipeline mode — rolling-snapshot semantics (N2 membership, N3 scalar value)
    // ------------------------------------------------------------------

    [Fact]
    public void PipelineSemiJoinCompileResultSourceNamesReportsRealLeafStreamsOnly()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol IN (SELECT symbol FROM hot) GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(["trades"], r.SourceNames);
    }

    [Fact]
    public void AcceptanceQuery_WithHotIn_NowFullyGreen()
    {
        // Plan 004's pinned acceptance example.
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t " +
                  "WHERE t.symbol IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Trades);

        // Round 1: establish hot's membership snapshot (these rows themselves are filtered — hot hasn't
        // closed yet, so its membership snapshot is still empty when they arrive; the plan 004 N2 "rolling
        // snapshot" rule is explicit that arrival is tested against whatever is CURRENTLY live, not a
        // future snapshot).
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1500, "trades", ("symbol", "AAPL"), ("price", 200.0), ("qty", 1L), ("active", true)));
        var duringClose = exec.AdvanceWatermark(11000); // closes hot's [0,10000) window -> snapshot = {AAPL}
        Assert.Empty(duringClose); // nothing was ever admitted into the outer's own window state yet

        // Round 2: NOW test against the populated snapshot.
        exec.OnEvent("trades", Evt(12000, "trades", ("symbol", "AAPL"), ("price", 300.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(12500, "trades", ("symbol", "MSFT"), ("price", 999.0), ("qty", 1L), ("active", true))); // MSFT never in hot's snapshot
        var results = exec.AdvanceWatermark(20000); // closes outer [10000,15000)

        var row = Assert.Single(results);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(300.0, row["px"]);
    }

    [Fact]
    public void PipelineMembershipSnapshotIsReplacedWholesaleOnEachWindowClose()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Trades);

        // Window 1 [0,10000): only AAPL present.
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.AdvanceWatermark(11000); // snapshot = {AAPL}

        // Window 2 [10000,20000): only MSFT present — the snapshot after this close must be REPLACED
        // (AAPL is no longer a member), not merged with window 1's membership.
        exec.OnEvent("trades", Evt(15000, "trades", ("symbol", "MSFT"), ("price", 40.0), ("qty", 1L), ("active", true)));
        exec.AdvanceWatermark(21000); // snapshot = {MSFT} (replaces {AAPL})

        exec.OnEvent("trades", Evt(22000, "trades", ("symbol", "AAPL"), ("price", 500.0), ("qty", 1L), ("active", true))); // AAPL no longer a member -> filtered
        exec.OnEvent("trades", Evt(22500, "trades", ("symbol", "MSFT"), ("price", 60.0), ("qty", 1L), ("active", true))); // MSFT still a member -> passes
        var results = exec.AdvanceWatermark(30000); // closes outer [20000,25000)

        var row = Assert.Single(results);
        Assert.Equal("MSFT", row["symbol"]);
        Assert.Equal(60.0, row["px"]);
    }

    [Fact]
    public void PipelineAntiJoinSnapshotFiltersMembersAndPassesNonMembers()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t WHERE t.symbol NOT IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.AdvanceWatermark(11000); // hot snapshot = {AAPL}

        exec.OnEvent("trades", Evt(12000, "trades", ("symbol", "AAPL"), ("price", 300.0), ("qty", 1L), ("active", true))); // member -> filtered by NOT IN
        exec.OnEvent("trades", Evt(12500, "trades", ("symbol", "MSFT"), ("price", 70.0), ("qty", 1L), ("active", true))); // non-member -> passes
        var results = exec.AdvanceWatermark(20000);

        var row = Assert.Single(results);
        Assert.Equal("MSFT", row["symbol"]);
        Assert.Equal(70.0, row["px"]);
    }

    [Fact]
    public void PipelineScalarSubquerySnapshotValueUsedAtArrival()
    {
        // Deliberately unwindowed at the OUTER level: a scalar subquery can't sit in the SELECT list of a
        // grouped/windowed/aggregated pipeline (see ScalarSubqueryInGroupedSelectListIsRejected — the same
        // TableReduceOp/PipelineWindowOp dummy-row limitation applies to windowed queries too, not just
        // grouped ones), but an UNWINDOWED outer pipeline projects immediately per event and is fine.
        var sql = "SELECT q.symbol, q.bid - (SELECT AVG(price) FROM trades WINDOW TUMBLING(SIZE 10 SECONDS)) AS rel FROM quotes q";
        var r = Compile(sql, Quotes, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        var exec = r.Plan!.CreateExecutor();

        // Before the inner window ever closes, the scalar's snapshot is empty -> NULL-padded (still emits;
        // a scalar subquery's value being NULL is not a row filter).
        var beforeClose = exec.OnEvent("quotes", Evt(500, "quotes", ("symbol", "AAPL"), ("bid", 150.0), ("ask", 151.0)));
        var beforeRow = Assert.Single(beforeClose);
        Assert.Null(beforeRow["rel"]);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.AdvanceWatermark(11000); // closes trades' [0,10000) window -> scalar snapshot value = 100

        var results = exec.OnEvent("quotes", Evt(12000, "quotes", ("symbol", "AAPL"), ("bid", 150.0), ("ask", 151.0)));
        var row = Assert.Single(results);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(50.0, row["rel"]); // 150 - 100
    }

    // ------------------------------------------------------------------
    // Determinism replay — table mode, semi-join + scalar subquery combined
    // ------------------------------------------------------------------

    private const string DeterminismSql =
        "SELECT t.symbol, t.price, (SELECT AVG(price) FROM trades) AS avgall FROM trades t WHERE t.symbol IN (SELECT symbol FROM ref)";

    private static List<IReadOnlyList<TableDelta>> RunDeterminismBatchSequence(TableExecutor exec)
    {
        return
        [
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1)),
            exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true))),
            exec.OnStreamEvent("trades", Evt(1500, "trades", ("symbol", "MSFT"), ("price", 5.0), ("qty", 1L), ("active", true))), // filtered by IN (MSFT not in ref)
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1)),
            exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 3L), ("active", true))),
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1)), // retraction cascades through the semi-join
        ];
    }

    private static string CanonDelta(IReadOnlyList<TableDelta> deltas) =>
        string.Join("|", deltas.Select(d => $"{d.Weight}:{Runtime.JsonText.SerializeCanonicalRow(d.Row)}"));

    [Fact]
    public void DeterminismReplay_SemiJoinPlusScalarSubquery_ReplaysIdenticallyAcrossFreshExecutors()
    {
        var compileResult = CompileTable(DeterminismSql, [Trades], [Ref]);
        Assert.True(compileResult.Ok, string.Join(";", compileResult.Diagnostics));
        var plan = compileResult.Plan!;

        var runs = Enumerable.Range(0, 3).Select(_ => RunDeterminismBatchSequence(plan.CreateExecutor())).ToList();

        var canonical = runs.Select(r => string.Join(";", r.Select(CanonDelta))).Distinct().ToList();
        Assert.Single(canonical);
    }
}
