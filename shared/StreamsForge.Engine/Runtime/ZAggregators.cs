namespace StreamsForge.Engine.Runtime;

/// <summary>Subtractable (Z-set-aware) aggregate accumulator: Add is replaced with Apply(value, weight),
/// where a negative weight retracts a previously-applied contribution. NULL inputs are skipped (except
/// COUNT(*), which counts rows regardless of value).</summary>
public interface IZAggregator
{
    void Apply(object? value, long weight);
    object? Result { get; }

    /// <summary>
    /// ADDITIVE — true when this accumulator's state is indistinguishable from a freshly created one, i.e.
    /// every contribution it has seen has been netted out. <see cref="Ops.TableReduceOp"/> drops a group
    /// whose total weight reaches zero ONLY when every aggregator also reports empty: a zero total weight
    /// alone does not mean the group's input multiset is empty when a retraction arrived before its own
    /// assertion ({+r2, −r1} has weight 0 but SUM r2 − r1), and dropping that state made GROUP BY results
    /// depend on delivery order (found by java/engine-dd's shuffled differential test).
    ///
    /// The default (<c>true</c>) keeps a third-party aggregate exactly as it behaved before this member
    /// existed — its group is dropped at weight zero — so implementing it is opt-in. An aggregate that
    /// can carry a negative (not-yet-asserted) contribution should override it.
    /// </summary>
    bool IsEmpty => true;
}

internal static class ZAggregator
{
    public static IZAggregator Create(Sql.AggregateCallExpr node)
    {
        string name = StatAggregatorNames.Canonical(node.Name) ?? node.Name;
        if (node.IsDistinct) return new DistinctCountAggregator();
        if (name == StatAggregatorNames.PercentileCont) return new PercentileZAggregator(AggregateParameters.Probability(node));
        return CreateCanonical(name, node.IsStar);
    }

    private static IZAggregator CreateCanonical(string name, bool isStar) => name switch
    {
        "COUNT" => new CountZAggregator(isStar),
        "SUM" => new SumZAggregator(),
        "AVG" => new AvgZAggregator(),
        "MIN" => new MinMaxZAggregator(isMin: true),
        "MAX" => new MinMaxZAggregator(isMin: false),
        StatAggregatorNames.VarSamp => new VarianceZAggregator(sample: true, stdDev: false),
        StatAggregatorNames.VarPop => new VarianceZAggregator(sample: false, stdDev: false),
        StatAggregatorNames.StdDevSamp => new VarianceZAggregator(sample: true, stdDev: true),
        StatAggregatorNames.StdDevPop => new VarianceZAggregator(sample: false, stdDev: true),
        StatAggregatorNames.Median => new MedianZAggregator(),
        // See Aggregator.Create: built-ins first, registry second, never the other way round.
        _ => Sql.SqlFunctions.FindAggregate(name)?.CreateZ()
             ?? throw new ArgumentException($"Unknown aggregate '{name}'"),
    };
}

internal sealed class CountZAggregator(bool isStar) : IZAggregator
{
    private long _count;
    public void Apply(object? value, long weight)
    {
        if (isStar || value is not null) _count += weight;
    }
    public object? Result => _count;
    public bool IsEmpty => _count == 0;
}

internal sealed class SumZAggregator : IZAggregator
{
    private double _sum;
    private long _longSum;
    private bool _sawDouble;

    public void Apply(object? value, long weight)
    {
        switch (value)
        {
            case double d: _sum += d * weight; _sawDouble = true; break;
            case long l: _sum += l * weight; _longSum += l * weight; break;
        }
    }

    public object? Result => _sawDouble ? (object)_sum : (object)_longSum;

    // SUM is linear: a state whose sums are both zero answers exactly like a fresh one for every future
    // contribution, whatever multiset produced it ({+5, −5} of two different rows included).
    public bool IsEmpty => _sum == 0 && _longSum == 0;
}

internal sealed class AvgZAggregator : IZAggregator
{
    private double _sum;
    private long _count;

    public void Apply(object? value, long weight)
    {
        switch (value)
        {
            case double d: _sum += d * weight; _count += weight; break;
            case long l: _sum += l * weight; _count += weight; break;
        }
    }

    public object? Result => _count == 0 ? null : _sum / _count;
    public bool IsEmpty => _count == 0 && _sum == 0;
}

/// <summary>Per-group SIGNED multiset of contributing values, ordered by SqlValues.Compare — MIN/MAX read
/// the first/last value whose count is positive. Retraction (negative weight) decrements the count; an
/// entry is pruned only when its count is exactly zero, so removing one of two equal minimums leaves the
/// min unchanged, and removing both exposes the next value. A value retracted BEFORE it was asserted is
/// kept at a negative count (it used to be ignored, so the later assertion resurrected a value the input
/// no longer held: rows {3, 4}, then −5, then +5 reported MAX 5 instead of 4) and is invisible to the
/// result until its assertion nets it out.</summary>
internal sealed class MinMaxZAggregator(bool isMin) : IZAggregator
{
    private readonly SortedDictionary<object, long> _multiset = new(Comparer<object>.Create(SqlValues.Compare));
    private int _negativeEntries;

    public void Apply(object? value, long weight)
    {
        if (value is null || weight == 0) return;
        long existing = _multiset.TryGetValue(value, out var e) ? e : 0;
        long newWeight = existing + weight;
        if (existing < 0) _negativeEntries--;
        if (newWeight < 0) _negativeEntries++;
        if (newWeight == 0) _multiset.Remove(value);
        else _multiset[value] = newWeight;
    }

    public object? Result
    {
        get
        {
            if (_multiset.Count == 0) return null;
            if (_negativeEntries == 0) return isMin ? _multiset.Keys.First() : _multiset.Keys.Last();
            IEnumerable<KeyValuePair<object, long>> ordered = isMin ? _multiset : _multiset.Reverse();
            foreach (var kv in ordered)
            {
                if (kv.Value > 0) return kv.Key;
            }
            return null;
        }
    }

    public bool IsEmpty => _multiset.Count == 0;
}
