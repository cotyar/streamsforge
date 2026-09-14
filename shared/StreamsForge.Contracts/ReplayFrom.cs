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

/// <summary>Plan 026 D6 — per-entity replay retention. Absent/null = the in-memory ring only (plan 026
/// waves 1–2: positions restart at 1 per activation). <see cref="Persist"/> turns on the disk-backed
/// segment log under <c>DataDir/replay/&lt;ns&gt;/&lt;qualified key&gt;/</c>, which makes positions durable
/// across a restart and lets a consumer replay from the start of retention. <see cref="RetainMs"/> /
/// <see cref="MaxBytes"/> bound it (0 = the host's <c>Replay:RetainMs</c> / <c>Replay:MaxBytesPerStream</c>
/// defaults). Independent of <c>TablePersistenceMode</c>: a <c>MemoryOnly</c> table with
/// <c>replay.persist</c> keeps its delta log on disk and nothing else. Dapr stores it and ignores it
/// (dapr/PARITY.md D11).</summary>
[GenerateSerializer]
public sealed record ReplayConfig
{
    [Id(0)] public bool Persist { get; set; }
    [Id(1)] public long RetainMs { get; set; }
    [Id(2)] public long MaxBytes { get; set; }
}

/// <summary>Plan 026 D6 — what <c>GET /api/{sources|pipelines|tables}/{key}/replay</c> reports: the
/// positions a producer still retains (memory + disk), and whether the disk tier is on.</summary>
[GenerateSerializer]
public sealed record ReplayStatus
{
    [Id(0)] public long FirstSeq { get; set; }
    [Id(1)] public long LastSeq { get; set; }
    [Id(2)] public long TotalSeen { get; set; }
    [Id(3)] public long RetainedBytes { get; set; }
    [Id(4)] public bool Persisted { get; set; }
    [Id(5)] public int Segments { get; set; }
}

/// <summary>Plan 026 D6 — the runtime seam behind the three <c>/replay</c> routes. Null = the entity is
/// not in the catalog; <see cref="NotSupportedException"/> = this flavor has no replay log at all (Dapr,
/// until PARITY D11 closes — the endpoint answers 501 with that message).</summary>
public interface IReplayStatusFacade
{
    Task<ReplayStatus?> GetSourceAsync(string sourceName);
    Task<ReplayStatus?> GetPipelineAsync(string pipelineId);
    Task<ReplayStatus?> GetTableAsync(string tableName);
}
