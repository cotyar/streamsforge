namespace StreamsForge.Engine.Runtime;

/// <summary>Incremental (streaming) aggregate accumulator. NULL inputs are skipped (except COUNT(*), which
/// counts rows regardless of value).</summary>
public abstract class Aggregator
{
    public abstract void Add(object? value);
    public abstract object? Result { get; }

    /// <summary>Internal even though the class is public: the extension seam is Add/Result (what a
    /// registered aggregate implements), not this factory, which reads a parsed call.</summary>
    internal static Aggregator Create(Sql.AggregateCallExpr node)
    {
        string name = StatAggregatorNames.Canonical(node.Name) ?? node.Name;
        if (node.IsDistinct) return new DistinctCountAggregator();
        if (name == StatAggregatorNames.PercentileCont) return new PercentileAggregator(AggregateParameters.Probability(node));
        return CreateCanonical(name, node.IsStar);
    }

    private static Aggregator CreateCanonical(string name, bool isStar) => name switch
    {
        "COUNT" => new CountAggregator(isStar),
        "SUM" => new SumAggregator(),
        "AVG" => new AvgAggregator(),
        "MIN" => new MinMaxAggregator(isMin: true),
        "MAX" => new MinMaxAggregator(isMin: false),
        StatAggregatorNames.VarSamp => new VarianceAggregator(sample: true, stdDev: false),
        StatAggregatorNames.VarPop => new VarianceAggregator(sample: false, stdDev: false),
        StatAggregatorNames.StdDevSamp => new VarianceAggregator(sample: true, stdDev: true),
        StatAggregatorNames.StdDevPop => new VarianceAggregator(sample: false, stdDev: true),
        StatAggregatorNames.Median => new MedianAggregator(),
        // Registered aggregates are consulted only after the built-ins, and SqlFunctions refuses to
        // register a built-in's name, so this lookup can never change what an existing query means.
        _ => Sql.SqlFunctions.FindAggregate(name)?.CreateStream()
             ?? throw new ArgumentException($"Unknown aggregate '{name}'"),
    };
}

internal sealed class CountAggregator(bool isStar) : Aggregator
{
    private long _count;
    public override void Add(object? value)
    {
        if (isStar || value is not null) _count++;
    }
    public override object? Result => _count;
}

internal sealed class SumAggregator : Aggregator
{
    private double _sum;
    private long _longSum;
    private bool _sawDouble;
    private bool _any;

    public override void Add(object? value)
    {
        switch (value)
        {
            case double d: _sum += d; _sawDouble = true; _any = true; break;
            case long l: _sum += l; _longSum += l; _any = true; break;
            case null: break;
        }
    }

    // PostgreSQL: SUM over no non-NULL value is NULL, not 0. Branches are boxed explicitly to avoid C#
    // unifying long/double arms to double before boxing.
    public override object? Result => !_any ? null : _sawDouble ? (object)_sum : (object)_longSum;
}

internal sealed class AvgAggregator : Aggregator
{
    private double _sum;
    private long _count;

    public override void Add(object? value)
    {
        switch (value)
        {
            case double d: _sum += d; _count++; break;
            case long l: _sum += l; _count++; break;
            case null: break;
        }
    }

    public override object? Result => _count == 0 ? null : _sum / _count;
}

internal sealed class MinMaxAggregator(bool isMin) : Aggregator
{
    private object? _best;
    private bool _any;

    public override void Add(object? value)
    {
        if (value is null) return;
        if (!_any) { _best = value; _any = true; return; }
        int cmp = SqlValues.Compare(value, _best!);
        if ((isMin && cmp < 0) || (!isMin && cmp > 0)) _best = value;
    }

    public override object? Result => _best;
}
