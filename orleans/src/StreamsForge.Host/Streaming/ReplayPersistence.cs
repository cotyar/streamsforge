using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Streaming;
using StreamsForge.Engine;

namespace StreamsForge.Host.Streaming;

/// <summary>Plan 026 D6 — the one place a producer grain turns its <c>replay.persist</c> flag into a
/// <see cref="ReplaySegmentLog"/>, so the five publish sites (connector, generator, ingest, pipeline
/// results, table deltas) cannot drift in where they put their segments or how they read the host
/// defaults.
///
/// <para>Directory layout is <c>{Replay:Directory}/&lt;stream namespace&gt;/&lt;sanitized grain key&gt;/</c>
/// — the namespace so the three kinds never collide on a shared name, and the SANITIZED key for exactly
/// the reason <see cref="Storage.JsonFileGrainStorage"/> sanitizes its state filenames: every character
/// outside <c>[A-Za-z0-9_.-]</c> becomes <c>_</c>. <c>.</c> survives because it is plan 021's environment
/// separator and collapsing it would merge <c>staging.orders</c> into the default environment's
/// <c>staging_orders</c>.</para>
///
/// <para>WRITE-BEHIND: every persisted grain arms ONE <see cref="FlushPeriod"/> grain timer calling the
/// gate's <c>FlushAsync</c> (single-flight lives inside the log), plus a flush on stop and on deactivate.
/// A dedicated timer rather than a ride on <c>TableGrain</c>'s existing <c>_flushTimer</c> on purpose:
/// that one is not registered at all for a <c>MemoryOnly</c> table, and a MemoryOnly table WITH
/// <c>replay.persist</c> still owes its delta log a flush (D6's "independent of TablePersistenceMode").
/// One rule for five grains beats one rule plus an exception.</para></summary>
internal static partial class ReplayPersistence
{
    /// <summary>Write-behind cadence, matching <c>TableGrain</c>'s own default flush interval.</summary>
    public static readonly TimeSpan FlushPeriod = TimeSpan.FromSeconds(2);

    [GeneratedRegex("[^A-Za-z0-9_.-]")]
    private static partial Regex InvalidPathChars();

    public static string Sanitize(string key) => InvalidPathChars().Replace(key, "_");

    /// <summary>The disk tier for this entity, or null when it did not opt in — which is every entity by
    /// default, and is what keeps waves 1–2 behaviour byte-for-byte.</summary>
    /// <param name="services">The grain's own <c>ServiceProvider</c>. A silo with no <c>Replay</c> section
    /// bound at all (a bare <c>TestCluster</c>) falls back to the option defaults rather than throwing.</param>
    /// <param name="ns">The stream namespace this producer publishes on (<see cref="StreamConstants"/>).</param>
    /// <param name="qualifiedKey">The grain's primary key — already environment-qualified.</param>
    public static ReplaySegmentLog? Open(IServiceProvider services, ReplayConfig? cfg, string ns, string qualifiedKey)
    {
        if (cfg is not { Persist: true })
        {
            return null;
        }

        var host = services.GetService<IOptions<ReplayOptions>>()?.Value ?? new ReplayOptions();
        // A definition's own bound wins only when it is set; 0 means "the host default", never "unbounded".
        var effective = new ReplayOptions
        {
            Directory = host.Directory,
            SegmentBytes = host.SegmentBytes,
            RetainMs = cfg.RetainMs > 0 ? cfg.RetainMs : host.RetainMs,
            MaxBytesPerStream = cfg.MaxBytes > 0 ? cfg.MaxBytes : host.MaxBytesPerStream,
        };

        var root = host.Directory is { Length: > 0 } d ? d : Path.Combine("data", "replay");
        return new ReplaySegmentLog(Path.Combine(root, ns, Sanitize(qualifiedKey)), effective);
    }

    /// <summary>A row goes to disk as its plain dictionary and comes back as one: <see cref="EventRecord"/>
    /// is a <c>Dictionary&lt;string, object?&gt;</c> subclass, and the normalizer
    /// <see cref="ReplayPayloadJson"/> applies is written against that shape.</summary>
    public static string SerializeRow(EventRecord row) =>
        ReplayPayloadJson.Serialize<Dictionary<string, object?>>(row);

    public static EventRecord DeserializeRow(string json) =>
        new(ReplayPayloadJson.Deserialize<Dictionary<string, object?>>(json));
}
