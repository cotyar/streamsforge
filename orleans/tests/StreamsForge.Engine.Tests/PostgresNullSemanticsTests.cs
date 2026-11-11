using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// SUM and NOT IN follow PostgreSQL's NULL rules (aligned 2026-11-11; java/engine-dd's differential tests
/// found the two deviations):
/// <list type="bullet">
/// <item>SUM over no non-NULL value (an all-NULL group) is NULL, like AVG/MIN/MAX; COUNT(x) stays 0.</item>
/// <item><c>x NOT IN (S)</c>: TRUE when S is empty (even for a NULL x); FALSE on a match; otherwise NULL —
/// filtered — when x is NULL or S contains a NULL. IN and NOT EXISTS are unaffected by NULLs in S.</item>
/// </list>
/// </summary>
public class PostgresNullSemanticsTests
{
    private static EventRecord Trade(string? symbol, long? qty, long ts = 1000) =>
        Evt(ts, "trades", ("symbol", symbol), ("price", 1.0), ("qty", qty), ("active", true));

    private static EventRecord RefRow(string? symbol, string tag = "x") => Evt(0, "ref", ("symbol", symbol), ("tag", tag));

    private static HashSet<string?> Symbols(TableExecutor exec) =>
        exec.Snapshot().Values.Where(r => r.Weight > 0).Select(r => (string?)r.Row["symbol"]).ToHashSet();

    // ---------------------------------------------------------------- SUM

    [Fact]
    public void TableSum_OverAnAllNullGroup_IsNull_WhileCountIsZero()
    {
        var exec = CompileTableAndCreate(
            "SELECT symbol, SUM(qty) AS s, COUNT(qty) AS c, COUNT(*) AS n FROM trades GROUP BY symbol", [Trades]);
        exec.OnStreamEvent("trades", Trade("A", null));
        exec.OnStreamEvent("trades", Trade("A", null));

        var row = Assert.Single(exec.Snapshot().Values).Row;
        Assert.Null(row["s"]);
        Assert.Equal(0L, row["c"]);
        Assert.Equal(2L, row["n"]);
    }

    [Fact]
    public void TableSum_BecomesNullAgainWhenItsLastNonNullValueIsRetracted()
    {
        var exec = CompileTableAndCreate("SELECT symbol, SUM(qty) AS s FROM trades GROUP BY symbol", [Trades]);
        exec.OnTableDelta("trades", new TableDelta(Trade("A", null), 1));
        exec.OnTableDelta("trades", new TableDelta(Trade("A", 5L), 1));
        Assert.Equal(5L, Assert.Single(exec.Snapshot().Values).Row["s"]);

        exec.OnTableDelta("trades", new TableDelta(Trade("A", 5L), -1));
        Assert.Null(Assert.Single(exec.Snapshot().Values).Row["s"]);
    }

    [Fact]
    public void TableSum_OfValuesThatCancelToZero_IsZeroNotNull()
    {
        var exec = CompileTableAndCreate("SELECT symbol, SUM(qty) AS s FROM trades GROUP BY symbol", [Trades]);
        exec.OnStreamEvent("trades", Trade("A", 5L));
        exec.OnStreamEvent("trades", Trade("A", -5L));
        Assert.Equal(0L, Assert.Single(exec.Snapshot().Values).Row["s"]);
    }

    [Fact]
    public void PipelineSum_OverAnAllNullWindowGroup_IsNull()
    {
        var exec = CompileAndCreate(
            "SELECT symbol, SUM(qty) AS s, COUNT(qty) AS c FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)", Trades);
        exec.OnEvent("trades", Trade("A", null, 1000));
        exec.OnEvent("trades", Trade("B", 3L, 1000));
        var rows = exec.AdvanceWatermark(10_000);

        var a = Assert.Single(rows, r => (string)r["symbol"]! == "A");
        Assert.Null(a["s"]);
        Assert.Equal(0L, a["c"]);
        Assert.Equal(3L, Assert.Single(rows, r => (string)r["symbol"]! == "B")["s"]);
    }

    // ---------------------------------------------------------------- NOT IN, table mode

    private const string NotInSql = "SELECT symbol FROM trades WHERE symbol NOT IN (SELECT symbol FROM ref)";
    private const string InSql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM ref)";

    [Fact]
    public void NotIn_ANullInTheSubquery_FiltersEveryNonMatchingRow_AndItsRetractionRestoresThem()
    {
        var exec = CompileTableAndCreate(NotInSql, [Trades], [Ref]);
        exec.OnTableDelta("ref", new TableDelta(RefRow("AAPL"), 1));
        exec.OnStreamEvent("trades", Trade("MSFT", 1L));
        exec.OnStreamEvent("trades", Trade("AAPL", 1L));
        Assert.Equal(["MSFT"], Symbols(exec));

        var poisoned = exec.OnTableDelta("ref", new TableDelta(RefRow(null), 1));
        Assert.Equal(-1, Assert.Single(poisoned).Weight);
        Assert.Empty(Symbols(exec));

        exec.OnStreamEvent("trades", Trade("IBM", 1L)); // arrives while S holds a NULL: filtered too
        Assert.Empty(Symbols(exec));

        exec.OnTableDelta("ref", new TableDelta(RefRow(null), -1));
        Assert.Equal(["MSFT", "IBM"], Symbols(exec));
    }

    [Fact]
    public void NotIn_ANullOuterValue_PassesOnlyWhileTheSubqueryIsEmpty()
    {
        var exec = CompileTableAndCreate(NotInSql, [Trades], [Ref]);
        exec.OnStreamEvent("trades", Trade(null, 1L));
        Assert.Equal([null], Symbols(exec)); // NULL NOT IN (empty) is TRUE

        exec.OnTableDelta("ref", new TableDelta(RefRow("AAPL"), 1));
        Assert.Empty(Symbols(exec)); // NULL NOT IN ('AAPL') is NULL

        exec.OnTableDelta("ref", new TableDelta(RefRow("AAPL"), -1));
        Assert.Equal([null], Symbols(exec));

        exec.OnTableDelta("ref", new TableDelta(RefRow(null), 1));
        Assert.Empty(Symbols(exec)); // NULL NOT IN (NULL) is NULL
    }

    [Fact]
    public void NotIn_ANullOnlySubquery_FiltersEverything_ButAnEmptyOnePassesEverything()
    {
        var exec = CompileTableAndCreate(NotInSql, [Trades], [Ref]);
        exec.OnStreamEvent("trades", Trade("MSFT", 1L));
        exec.OnStreamEvent("trades", Trade(null, 1L));
        Assert.Equal(["MSFT", null], Symbols(exec));

        exec.OnTableDelta("ref", new TableDelta(RefRow(null), 1));
        Assert.Empty(Symbols(exec));
    }

    [Fact]
    public void NotIn_ConvergesWhateverTheOrderOfSubqueryAndOuterDeltas()
    {
        var deltas = new List<(string Input, TableDelta Delta)>
        {
            ("ref", new TableDelta(RefRow("AAPL"), 1)),
            ("ref", new TableDelta(RefRow(null, "n1"), 1)),
            ("ref", new TableDelta(RefRow(null, "n1"), -1)),
            ("trades", new TableDelta(Trade("AAPL", 1L), 1)),
            ("trades", new TableDelta(Trade("MSFT", 1L), 1)),
            ("trades", new TableDelta(Trade(null, 1L), 1)),
        };
        var rng = new Random(7);
        for (int round = 0; round < 50; round++)
        {
            var exec = CompileTableAndCreate(NotInSql, [Trades], [Ref]);
            foreach (var (input, delta) in deltas.OrderBy(_ => rng.Next())) exec.OnTableDelta(input, delta);
            Assert.Equal(["MSFT"], Symbols(exec));
        }
    }

    [Fact]
    public void In_IgnoresNullsOnBothSides()
    {
        var exec = CompileTableAndCreate(InSql, [Trades], [Ref]);
        exec.OnTableDelta("ref", new TableDelta(RefRow("AAPL"), 1));
        exec.OnTableDelta("ref", new TableDelta(RefRow(null), 1));
        exec.OnStreamEvent("trades", Trade("AAPL", 1L));
        exec.OnStreamEvent("trades", Trade("MSFT", 1L));
        exec.OnStreamEvent("trades", Trade(null, 1L));
        Assert.Equal(["AAPL"], Symbols(exec));
    }

    [Fact]
    public void NotExists_IsUnaffectedByNullsInTheSubquery()
    {
        var exec = CompileTableAndCreate(
            "SELECT symbol FROM trades WHERE NOT EXISTS (SELECT symbol FROM ref)", [Trades], [Ref]);
        exec.OnStreamEvent("trades", Trade("MSFT", 1L));
        Assert.Equal(["MSFT"], Symbols(exec));
        exec.OnTableDelta("ref", new TableDelta(RefRow(null), 1)); // S is non-empty: NOT EXISTS is FALSE
        Assert.Empty(Symbols(exec));
    }

    // ---------------------------------------------------------------- NOT IN, pipeline mode

    [Fact]
    public void PipelineNotIn_ANullInTheSubquerySnapshot_FiltersNonMatchingRows()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, COUNT(*) AS n FROM trades t WHERE t.symbol NOT IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Trade("AAPL", 1L, 1000));
        exec.OnEvent("trades", Trade(null, 1L, 1500));
        exec.AdvanceWatermark(11000); // hot snapshot = {AAPL, NULL}

        exec.OnEvent("trades", Trade("MSFT", 1L, 12500)); // no match, but S holds a NULL -> NULL -> filtered
        Assert.DoesNotContain(exec.AdvanceWatermark(20000), r => (string?)r["symbol"] == "MSFT");
    }

    [Fact]
    public void PipelineNotIn_ANullOuterValue_PassesOnlyBeforeTheSubqueryHasEmittedAnything()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, COUNT(*) AS n FROM trades t WHERE t.symbol NOT IN (SELECT symbol FROM hot) " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Trade(null, 1L, 1000)); // S still empty: NULL NOT IN () is TRUE
        Assert.Contains(exec.AdvanceWatermark(11000), r => r["symbol"] is null);

        exec.OnEvent("trades", Trade(null, 1L, 12000)); // S = {NULL} now: filtered
        Assert.DoesNotContain(exec.AdvanceWatermark(30000), r => r["symbol"] is null);
    }
}
