namespace StreamsForge.Engine.Runtime;

/// <summary>
/// The statistical aggregates: VAR_SAMP/VAR_POP/STDDEV_SAMP/STDDEV_POP (with the bare STDDEV/VARIANCE
/// spellings aliased to the _SAMP forms, which is what SQL means by them) and MEDIAN.
///
/// <para>Both families are built to be <b>subtractable</b>, because this dialect runs the same SQL two
/// ways and the table path maintains a Z-set: a row superseded by LATEST BY comes back with weight −1.
/// An aggregate that only knows how to add would keep a retracted row's contribution forever, and the
/// error is silent — the number just drifts.</para>
/// </summary>
internal static class StatAggregatorNames
{
    public const string VarSamp = "VAR_SAMP";
    public const string VarPop = "VAR_POP";
    public const string StdDevSamp = "STDDEV_SAMP";
    public const string StdDevPop = "STDDEV_POP";
    public const string Median = "MEDIAN";
    public const string PercentileCont = "PERCENTILE_CONT";

    /// <summary>The bare spellings, mapped to what SQL means by them (the sample forms).</summary>
    public static string? Canonical(string upperName) => upperName switch
    {
        "VARIANCE" or "VAR" => VarSamp,
        "STDDEV" or "STDEV" => StdDevSamp,
        VarSamp or VarPop or StdDevSamp or StdDevPop or Median or PercentileCont => upperName,
        _ => null,
    };

    public static readonly string[] All =
    [
        VarSamp, VarPop, StdDevSamp, StdDevPop, "VARIANCE", "VAR", "STDDEV", "STDEV", Median, PercentileCont,
    ];

    /// <summary>Aggregates written `NAME(parameter, value)` — the parameter is a constant that
    /// configures the aggregate for the whole group, not a second thing to aggregate.</summary>
    public static bool TakesParameter(string name) =>
        string.Equals(name, PercentileCont, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Running moments in the subtractable (n, Σy, Σy²) form, where y = x − K for a fixed offset K taken
/// from the first value this accumulator ever sees.
///
/// <para>The offset is not decoration. The textbook Σx²/n − mean² form loses catastrophic precision when
/// the values are large relative to their spread — prices around 1e6 with a spread of 0.01 cancel away
/// most of a double's mantissa — and Welford's numerically-stable update, the usual answer, cannot
/// subtract, so it is unusable on the Z path. Shifting by a constant keeps the subtractability (every
/// term is still a plain sum) while making the sums small, which is the whole problem. K never changes
/// once set, including across retractions, or terms accumulated under different offsets would not
/// cancel.</para>
/// </summary>
internal sealed class Moments
{
    private double _offset;
    private bool _haveOffset;
    private double _sum;
    private double _sumSquares;
    private long _count;

    public void Apply(object? value, long weight)
    {
        if (!SqlValues.IsNumber(value)) return;
        double x = SqlValues.ToDouble(value!);
        if (!_haveOffset) { _offset = x; _haveOffset = true; }
        double y = x - _offset;
        _count += weight;
        _sum += y * weight;
        _sumSquares += y * y * weight;
    }

    public long Count => _count;

    /// <summary>All three moments are linear in the contributions, so zero sums answer like a fresh
    /// accumulator (the offset only re-centres; a later value re-derives the same results).</summary>
    public bool IsEmpty => _count == 0 && _sum == 0 && _sumSquares == 0;

    /// <summary>Population variance, or null when there is nothing to describe. Clamped at zero: the
    /// sums form can produce a tiny negative from rounding when every value is identical, and a negative
    /// variance would become NaN in STDDEV.</summary>
    public double? PopulationVariance()
    {
        if (_count <= 0) return null;
        double mean = _sum / _count;
        return Math.Max(0d, _sumSquares / _count - mean * mean);
    }

    /// <summary>Sample variance — null below two observations, where it is undefined rather than zero.</summary>
    public double? SampleVariance()
    {
        if (_count < 2) return null;
        double mean = _sum / _count;
        return Math.Max(0d, (_sumSquares - _sum * mean) / (_count - 1));
    }

    public object? Result(bool sample, bool stdDev)
    {
        double? variance = sample ? SampleVariance() : PopulationVariance();
        if (variance is not { } v) return null;
        return stdDev ? Math.Sqrt(v) : v;
    }
}

internal sealed class VarianceAggregator(bool sample, bool stdDev) : Aggregator
{
    private readonly Moments _moments = new();
    public override void Add(object? value) => _moments.Apply(value, 1);
    public override object? Result => _moments.Result(sample, stdDev);
}

internal sealed class VarianceZAggregator(bool sample, bool stdDev) : IZAggregator
{
    private readonly Moments _moments = new();
    public void Apply(object? value, long weight) => _moments.Apply(value, weight);
    public object? Result => _moments.Result(sample, stdDev);
    public bool IsEmpty => _moments.IsEmpty;
}

/// <summary>
/// A weight-aware ordered multiset of numeric values — the shape a quantile needs and a running sum
/// cannot provide. Retraction decrements a value's count and prunes the entry at zero, exactly as
/// <c>MinMaxZAggregator</c> already does, so removing one of two equal values leaves the quantile
/// unchanged and removing both exposes its neighbour.
///
/// <para>ponytail: exact, over a SortedDictionary — O(n) to read a quantile. Correct at the sizes this
/// serves (per-group MC paths, ~10^4 rows) and honest about it; the upgrade path when that stops being
/// true is a t-digest behind this same class, not a different aggregate.</para>
/// </summary>
internal sealed class NumericMultiset
{
    // SIGNED counts (see MinMaxZAggregator): a value retracted before it was asserted keeps a negative
    // count until its assertion nets it out, instead of being ignored and then resurrected. _total is
    // the sum of POSITIVE counts only — the population the quantile is read from.
    private readonly SortedDictionary<double, long> _counts = [];
    private long _total;

    public void Apply(object? value, long weight)
    {
        if (!SqlValues.IsNumber(value) || weight == 0) return;
        double x = SqlValues.ToDouble(value!);
        long existing = _counts.TryGetValue(x, out var e) ? e : 0;
        long next = existing + weight;
        if (next == 0) _counts.Remove(x);
        else _counts[x] = next;
        _total += Math.Max(next, 0) - Math.Max(existing, 0);
    }

    public bool IsEmpty => _counts.Count == 0;

    /// <summary>Continuous quantile with linear interpolation between the two bracketing observations —
    /// the PERCENTILE_CONT definition, of which MEDIAN is p = 0.5. Null on an empty multiset.</summary>
    public object? Quantile(double p)
    {
        if (_counts.Count == 0 || _total <= 0) return null;
        if (p <= 0) return _counts.First(kv => kv.Value > 0).Key;
        if (p >= 1) return _counts.Last(kv => kv.Value > 0).Key;

        // Rank in [0, n-1] of the quantile, then the two observations that bracket it.
        double rank = p * (_total - 1);
        long lowerIndex = (long)Math.Floor(rank);
        double fraction = rank - lowerIndex;

        double lower = 0, upper = 0;
        long seen = 0;
        bool haveLower = false;
        foreach (var (value, count) in _counts)
        {
            if (count <= 0) continue;
            seen += count;
            if (!haveLower && seen > lowerIndex)
            {
                lower = value;
                haveLower = true;
                // The next index falls inside this same run whenever the run extends past it.
                if (seen > lowerIndex + 1) { upper = value; return Interpolate(lower, upper, fraction); }
                continue;
            }
            if (haveLower) { upper = value; return Interpolate(lower, upper, fraction); }
        }
        return haveLower ? lower : null;
    }

    private static object Interpolate(double lower, double upper, double fraction) =>
        fraction <= 0 ? lower : lower + (upper - lower) * fraction;
}

internal sealed class PercentileAggregator(double p) : Aggregator
{
    private readonly NumericMultiset _values = new();
    public override void Add(object? value) => _values.Apply(value, 1);
    public override object? Result => _values.Quantile(p);
}

internal sealed class PercentileZAggregator(double p) : IZAggregator
{
    private readonly NumericMultiset _values = new();
    public void Apply(object? value, long weight) => _values.Apply(value, weight);
    public object? Result => _values.Quantile(p);
    public bool IsEmpty => _values.IsEmpty;
}

/// <summary>
/// COUNT(DISTINCT x): a multiset of the values seen, whose Result is how many distinct ones still have
/// a positive count. Retraction decrements and prunes at zero, so a value that arrives twice and is
/// retracted once still counts, and one retracted to nothing stops counting — the reason this cannot be
/// a plain HashSet even on the stream path, where the two would otherwise agree.
/// </summary>
internal sealed class DistinctCountAggregator : Aggregator, IZAggregator
{
    // SIGNED counts (see MinMaxZAggregator): a retraction that arrives before its assertion is kept as a
    // negative count rather than ignored, so the later assertion nets it out instead of counting a value
    // the input no longer holds. Only positive counts are distinct values.
    private readonly Dictionary<object, long> _counts = new(SqlValueEqualityComparer.Instance);
    private long _positive;

    public override void Add(object? value) => Apply(value, 1);

    public void Apply(object? value, long weight)
    {
        if (value is null || weight == 0) return;
        long existing = _counts.TryGetValue(value, out var e) ? e : 0;
        long next = existing + weight;
        if (next == 0) _counts.Remove(value);
        else _counts[value] = next;
        _positive += (next > 0 ? 1 : 0) - (existing > 0 ? 1 : 0);
    }

    public override object? Result => _positive;
    public bool IsEmpty => _counts.Count == 0;
}

/// <summary>Equality for DISTINCT keys that agrees with <see cref="SqlValues.Compare"/> — notably that
/// the long 1 and the double 1.0 are the same value, which <see cref="object.Equals(object)"/> would
/// disagree with, giving a count of 2 for one distinct number.</summary>
internal sealed class SqlValueEqualityComparer : IEqualityComparer<object>
{
    public static readonly SqlValueEqualityComparer Instance = new();

    public new bool Equals(object? x, object? y) =>
        x is not null && y is not null && SqlValues.Compare(x, y) == 0;

    public int GetHashCode(object obj) =>
        SqlValues.IsNumber(obj) ? SqlValues.ToDouble(obj).GetHashCode() : obj.GetHashCode();
}

internal sealed class MedianAggregator : Aggregator
{
    private readonly NumericMultiset _values = new();
    public override void Add(object? value) => _values.Apply(value, 1);
    public override object? Result => _values.Quantile(0.5);
}

internal sealed class MedianZAggregator : IZAggregator
{
    private readonly NumericMultiset _values = new();
    public void Apply(object? value, long weight) => _values.Apply(value, weight);
    public object? Result => _values.Quantile(0.5);
    public bool IsEmpty => _values.IsEmpty;
}

/// <summary>Reads a parameterised aggregate's constant out of the parsed call. The Validator has
/// already rejected a missing or non-constant parameter by the time an executor is built, so the
/// fallback here is unreachable in a compiled plan — it exists so this is total rather than throwing
/// in the middle of a row.</summary>
internal static class AggregateParameters
{
    public static double Probability(Sql.AggregateCallExpr node) =>
        node.Parameter is Sql.NumberLiteral n && SqlValues.IsNumber(n.Value) ? SqlValues.ToDouble(n.Value) : 0.5;

    /// <summary>The Validator's own check, kept next to the reader so the two cannot disagree about
    /// what a usable parameter is.</summary>
    public static bool IsUsableProbability(Sql.Expr? parameter) =>
        parameter is Sql.NumberLiteral n && SqlValues.IsNumber(n.Value)
        && SqlValues.ToDouble(n.Value) is >= 0 and <= 1;
}
