using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Table-mode GROUP BY must not depend on the order in which a row's assertion and its retraction arrive
/// (a Z-set is a multiset: +r then −r and −r then +r are the same change). Found by java/engine-dd's
/// shuffled differential test against an independent differential-dataflow implementation; before the fix
/// TableReduceOp dropped a group at total weight exactly 0 even when its aggregators still held
/// {+r2, −r1}, and MIN/MAX, COUNT(DISTINCT) and the quantile multiset ignored a retraction of a value they
/// had not seen yet.
/// </summary>
public class TableReduceOrderInsensitivityTests
{
    private static TableExecutor Create(string sql) => CompileTableAndCreate(sql, [], [Trades]);

    private static EventRecord Trade(string symbol, double price, long? qty) =>
        Evt(0, "trades", ("symbol", symbol), ("price", price), ("qty", qty), ("active", true));

    private static object? Only(TableExecutor exec, string column) => Assert.Single(exec.Snapshot().Values).Row[column];

    [Fact]
    public void RetractionBeforeAssertion_WithTheGroupAtZeroWeight_KeepsTheAggregateState()
    {
        var exec = Create("SELECT symbol, SUM(qty) AS s, MAX(qty) AS mx, MIN(qty) AS mn, AVG(qty) AS a FROM trades GROUP BY symbol");
        exec.OnTableDelta("trades", new TableDelta(Trade("A", 1.0, 5L), -1)); // r1 retracted first
        exec.OnTableDelta("trades", new TableDelta(Trade("A", 2.0, 7L), 1));  // r2 → weight 0, state {+r2, −r1}
        Assert.Empty(exec.Snapshot());                                         // a zero-weight group shows nothing
        exec.OnTableDelta("trades", new TableDelta(Trade("A", 1.0, 5L), 1));  // r1's assertion: the input is {r2}

        Assert.Equal(7L, Only(exec, "s"));
        Assert.Equal(7L, Only(exec, "mx"));
        Assert.Equal(7L, Only(exec, "mn"));
        Assert.Equal(7.0, Only(exec, "a"));
    }

    [Fact]
    public void MaxIgnoresNothing_ARetractionOfAnUnseenValueIsNettedByItsLaterAssertion()
    {
        var exec = Create("SELECT symbol, MAX(qty) AS mx, MIN(qty) AS mn, COUNT(*) AS c FROM trades GROUP BY symbol");
        exec.OnTableDelta("trades", new TableDelta(Trade("B", 1.0, 3L), 1));
        exec.OnTableDelta("trades", new TableDelta(Trade("B", 2.0, 4L), 1));
        exec.OnTableDelta("trades", new TableDelta(Trade("B", 3.0, 5L), -1)); // the group stays alive throughout
        exec.OnTableDelta("trades", new TableDelta(Trade("B", 3.0, 5L), 1));

        Assert.Equal(4L, Only(exec, "mx"));
        Assert.Equal(3L, Only(exec, "mn"));
        Assert.Equal(2L, Only(exec, "c"));
    }

    [Fact]
    public void CountDistinctAndMedian_AreNotResurrectedByAnEarlyRetraction()
    {
        var exec = Create("SELECT symbol, COUNT(DISTINCT qty) AS d, MEDIAN(qty) AS md FROM trades GROUP BY symbol");
        exec.OnTableDelta("trades", new TableDelta(Trade("C", 1.0, 10L), 1));
        exec.OnTableDelta("trades", new TableDelta(Trade("C", 2.0, 20L), 1));
        exec.OnTableDelta("trades", new TableDelta(Trade("C", 3.0, 90L), -1));
        exec.OnTableDelta("trades", new TableDelta(Trade("C", 3.0, 90L), 1));

        Assert.Equal(2L, Only(exec, "d"));
        Assert.Equal(15.0, Only(exec, "md"));
    }

    [Fact]
    public void AGroupWhoseRowsAllNetOut_IsStillDropped()
    {
        var exec = Create("SELECT symbol, SUM(qty) AS s, MAX(qty) AS mx FROM trades GROUP BY symbol");
        exec.OnTableDelta("trades", new TableDelta(Trade("D", 1.0, 5L), -1));
        exec.OnTableDelta("trades", new TableDelta(Trade("D", 1.0, 5L), 1));
        Assert.Empty(exec.Snapshot());

        // A later row starts the group from scratch — nothing of the netted-out pair leaks into it.
        exec.OnTableDelta("trades", new TableDelta(Trade("D", 2.0, 8L), 1));
        Assert.Equal(8L, Only(exec, "s"));
        Assert.Equal(8L, Only(exec, "mx"));
    }

    /// <summary>The property itself: any permutation of the same multiset of deltas converges to the same
    /// snapshot as the in-order (assert-before-retract) sequence.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void AnyDeliveryOrderOfTheSameDeltas_ConvergesToTheSameSnapshot(int seed)
    {
        const string sql = "SELECT symbol, COUNT(*) AS c, COUNT(qty) AS cq, SUM(qty) AS s, AVG(qty) AS a, MIN(qty) AS mn, "
            + "MAX(qty) AS mx, COUNT(DISTINCT qty) AS d, MEDIAN(qty) AS md FROM trades GROUP BY symbol";
        var rng = new Random(seed);
        var deltas = new List<TableDelta>();
        for (int i = 0; i < 60; i++)
        {
            var row = Trade(((char)('A' + rng.Next(3))).ToString(), i, rng.Next(4) == 0 ? null : rng.Next(1, 6));
            deltas.Add(new TableDelta(row, 1));
            if (rng.Next(3) == 0) deltas.Add(new TableDelta(row, -1)); // retracted later in the in-order run
        }

        var inOrder = Create(sql);
        foreach (var d in deltas) inOrder.OnTableDelta("trades", d);
        var expected = inOrder.Snapshot();

        for (int round = 0; round < 20; round++)
        {
            var shuffled = deltas.OrderBy(_ => rng.Next()).ToList();
            var exec = Create(sql);
            foreach (var d in shuffled) exec.OnTableDelta("trades", d);
            var actual = exec.Snapshot();

            Assert.Equal(expected.Keys.OrderBy(k => k, StringComparer.Ordinal), actual.Keys.OrderBy(k => k, StringComparer.Ordinal));
            foreach (var (key, value) in expected) Assert.Equal(value.Weight, actual[key].Weight);
        }
    }

    /// <summary>VAR_POP gets its own check because exact equality is the wrong bar for it: its moments are
    /// re-centred on the FIRST value an accumulator sees, so a different delivery order legitimately rounds
    /// differently in the last bits. Order-insensitivity there means "same groups, same value within
    /// floating tolerance".</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void VarianceIsOrderInsensitiveWithinFloatingTolerance(int seed)
    {
        const string sql = "SELECT symbol, VAR_POP(qty) AS v, STDDEV_SAMP(qty) AS sd FROM trades GROUP BY symbol";
        var rng = new Random(seed);
        var deltas = new List<TableDelta>();
        for (int i = 0; i < 60; i++)
        {
            var row = Trade(((char)('A' + rng.Next(3))).ToString(), i, rng.Next(1, 6));
            deltas.Add(new TableDelta(row, 1));
            if (rng.Next(3) == 0) deltas.Add(new TableDelta(row, -1));
        }

        static Dictionary<string, (double? V, double? Sd)> BySymbol(TableExecutor exec) =>
            exec.Snapshot().Values.ToDictionary(r => (string)r.Row["symbol"]!, r => ((double?)r.Row["v"], (double?)r.Row["sd"]));

        var inOrder = Create(sql);
        foreach (var d in deltas) inOrder.OnTableDelta("trades", d);
        var expected = BySymbol(inOrder);

        for (int round = 0; round < 20; round++)
        {
            var exec = Create(sql);
            foreach (var d in deltas.OrderBy(_ => rng.Next())) exec.OnTableDelta("trades", d);
            var actual = BySymbol(exec);

            Assert.Equal(expected.Keys.Order(), actual.Keys.Order());
            foreach (var (symbol, (v, sd)) in expected)
            {
                Assert.Equal(v!.Value, actual[symbol].V!.Value, 1e-9);
                Assert.Equal(sd!.Value, actual[symbol].Sd!.Value, 1e-9);
            }
        }
    }
}
