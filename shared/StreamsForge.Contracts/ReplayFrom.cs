namespace StreamsForge.Abstractions;

/// <summary>Plan 026 — where a replay should start. <see cref="Seq"/> is a producer-assigned stream
/// position (the <c>position</c> field on the wire, 1-based, monotonic per stream); <see cref="TimestampMs"/>
/// is the entry's own event time (the reserved <c>_ts</c>). When both are set, <see cref="Seq"/> wins —
/// a position is exact, a timestamp is a filter. <c>null</c> (or a default instance) means "everything
/// the producer still retains", which is what plan 023's parameterless attach always meant.</summary>
[GenerateSerializer]
public sealed record ReplayFrom
{
    [Id(0)] public long? Seq { get; set; }
    [Id(1)] public long? TimestampMs { get; set; }

    public bool IsEmpty => Seq is null && TimestampMs is null;
}
