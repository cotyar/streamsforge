namespace StreamsForge.Engine.Runtime;

/// <summary>A Z-set index for one side of a table-mode equi-join: join-key → (canonical row text → (row,
/// weight)). Weight is accumulated per distinct row so repeated identical deltas net out instead of
/// growing the bucket unboundedly; entries whose weight nets to zero are pruned immediately.</summary>
internal sealed class ZSetIndex
{
    private readonly Dictionary<string, Dictionary<string, (WorkingRow Row, long Weight)>> _byKey = [];

    /// <summary>Every key that has ever held a row (a bucket may since have netted to empty).</summary>
    public IEnumerable<string> Keys => _byKey.Keys;

    public IEnumerable<(WorkingRow Row, long Weight)> Lookup(string key)
    {
        if (!_byKey.TryGetValue(key, out var bucket)) yield break;
        foreach (var kv in bucket)
        {
            if (kv.Value.Weight != 0) yield return kv.Value;
        }
    }

    public void Apply(string key, string rowCanonical, WorkingRow row, long weight)
    {
        if (!_byKey.TryGetValue(key, out var bucket))
        {
            bucket = [];
            _byKey[key] = bucket;
        }

        if (bucket.TryGetValue(rowCanonical, out var existing))
        {
            long newWeight = existing.Weight + weight;
            if (newWeight == 0) bucket.Remove(rowCanonical);
            else bucket[rowCanonical] = (row, newWeight);
        }
        else if (weight != 0)
        {
            bucket[rowCanonical] = (row, weight);
        }
    }
}
