using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

internal sealed class AuditTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
}

internal sealed class AuditTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) { }
}

/// <summary>
/// Plan 015 W4-B — the Orleans audit log: day-sharded, bounded, and honest about what it dropped.
///
/// <para><c>Audit:MaxEntriesPerDay</c> is set to 5 for this whole cluster, which is what makes the cap
/// testable at all: the 20 000 default would need 20 001 grain calls to reach, and the code path is
/// identical. The knob is read at activation, so every day grain in this class caps at five.</para>
///
/// <para>Each test picks its own fake calendar days (a distinct YEAR each), because the grain key IS the
/// day — two tests sharing a day would share an activation.</para>
/// </summary>
public sealed class AuditLogGrainTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            [AuditLogGrain.MaxEntriesPerDayKey] = "5",
        }));
        builder.AddSiloBuilderConfigurator<AuditTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<AuditTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private IAuditFacade Facade => new OrleansAuditFacade(_cluster.Client);

    private static long AtUtc(int year, int month, int day, int hour = 12, int minute = 0) =>
        new DateTimeOffset(year, month, day, hour, minute, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();

    private static AuditEntry Entry(long atMs, string actor = "alice", string action = "pipeline.update", string scope = "prod-a") => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = atMs,
        Actor = actor,
        Action = action,
        Scope = scope,
        Outcome = "allowed",
    };

    // ------------------------------------------------------------------------------------------
    // The cap
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Drop-OLDEST, with a counter. The direction is the opposite of the sink's bounded channel
    /// (drop-write) and deliberately so: the sink's competing rows are milliseconds apart, these are a
    /// whole day apart.
    ///
    /// <para>The counter is the point. A log that silently forgot three rows would make "nothing was
    /// recorded between 09:00 and 09:05" indistinguishable from "nothing happened", which is exactly the
    /// silence <see cref="AuditPage.Truncated"/> exists to break — so it is asserted on the page, not
    /// merely believed.</para>
    /// </summary>
    [Fact]
    public async Task PastTheCap_TheOldestEntriesAreDropped_AndTruncatedCounts()
    {
        var day = AtUtc(2001, 1, 1);
        for (var i = 0; i < 8; i++)
        {
            await Facade.AppendAsync(Entry(day + i, action: $"a{i}.act"));
        }

        var page = await Facade.QueryAsync("20010101", null, null, 100, 0);

        Assert.Equal(5, page.Entries.Count);
        Assert.Equal(5, page.Total);
        Assert.Equal(3, page.Truncated);
        // Newest first, and the three oldest are the ones that are gone.
        Assert.Equal(["a7.act", "a6.act", "a5.act", "a4.act", "a3.act"], page.Entries.Select(e => e.Action).ToArray());
    }

    /// <summary>The counter is cumulative and never resets — a day that has dropped rows carries that
    /// fact for as long as it exists, so a caller reading page 1 tomorrow still learns there is a
    /// hole.</summary>
    [Fact]
    public async Task Truncated_IsCumulative_AcrossLaterAppends()
    {
        var day = AtUtc(2002, 2, 2);
        for (var i = 0; i < 7; i++)
        {
            await Facade.AppendAsync(Entry(day + i));
        }

        Assert.Equal(2, (await Facade.QueryAsync("20020202", null, null, 100, 0)).Truncated);

        await Facade.AppendAsync(Entry(day + 100));
        Assert.Equal(3, (await Facade.QueryAsync("20020202", null, null, 100, 0)).Truncated);
    }

    // ------------------------------------------------------------------------------------------
    // Day sharding
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Two days land in two grains, and neither can see the other.
    ///
    /// <para>That is the whole point of the key format: a day activates only when written to or read and
    /// is collected when idle. A store that quietly kept one list would pass every other test in this
    /// file and would grow forever.</para>
    ///
    /// <para>Note the append is routed by the ENTRY's timestamp, not by the clock at drain time, so a row
    /// that sat in the sink's queue across midnight lands in the day it happened.</para>
    /// </summary>
    [Fact]
    public async Task TwoDays_LandInTwoDifferentGrains()
    {
        await Facade.AppendAsync(Entry(AtUtc(2003, 3, 3), action: "day3.act"));
        await Facade.AppendAsync(Entry(AtUtc(2003, 3, 4), action: "day4.act"));

        var third = await Facade.QueryAsync("20030303", null, null, 100, 0);
        var fourth = await Facade.QueryAsync("20030304", null, null, 100, 0);

        Assert.Equal(["day3.act"], third.Entries.Select(e => e.Action).ToArray());
        Assert.Equal(["day4.act"], fourth.Entries.Select(e => e.Action).ToArray());

        // And the grains really are distinct activations under the documented keys — asserted directly
        // rather than inferred, since the facade's routing rule is the thing under test.
        var direct = await _cluster.GrainFactory
            .GetGrain<IAuditLogGrain>(StreamConstants.AuditKeyFor(AtUtc(2003, 3, 4)))
            .QueryAsync("20030304", null, null, 100, 0);
        Assert.Equal(["day4.act"], direct.Entries.Select(e => e.Action).ToArray());
    }

    /// <summary>The day index answers "which days have entries" without touching a day grain, newest
    /// first — <c>yyyyMMdd</c> sorts chronologically as a string, which is why the key format is that
    /// one. A day that was only ever QUERIED must not appear: a read cannot invent a day.</summary>
    [Fact]
    public async Task GetDays_ListsWrittenDaysNewestFirst_AndNeverAQueriedOne()
    {
        await Facade.AppendAsync(Entry(AtUtc(2004, 4, 4)));
        await Facade.AppendAsync(Entry(AtUtc(2004, 6, 6)));
        await Facade.AppendAsync(Entry(AtUtc(2004, 5, 5)));

        // A day nobody ever wrote to, read anyway.
        await Facade.QueryAsync("20040707", null, null, 10, 0);

        var days = await Facade.GetDaysAsync();

        // Other tests in this class write their own days into the same shared index, so this asserts
        // relative order and membership rather than the whole list.
        var mine = days.Where(d => d.StartsWith("2004", StringComparison.Ordinal)).ToArray();
        Assert.Equal(["20040606", "20040505", "20040404"], mine);
        Assert.DoesNotContain("20040707", days);
    }

    // ------------------------------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------------------------------

    /// <summary>Exact-match on actor, prefix-match on action — the two filters the facade promises, and
    /// nothing richer, because "anything richer is a query engine this platform already is, one layer
    /// up".</summary>
    [Fact]
    public async Task Query_FiltersExactOnActorAndPrefixOnAction()
    {
        var day = AtUtc(2005, 5, 5);
        await Facade.AppendAsync(Entry(day + 1, actor: "alice", action: "pipeline.update"));
        await Facade.AppendAsync(Entry(day + 2, actor: "bob", action: "pipeline.delete"));
        await Facade.AppendAsync(Entry(day + 3, actor: "alice", action: "table.read"));

        var byActor = await Facade.QueryAsync("20050505", "alice", null, 100, 0);
        Assert.Equal(["table.read", "pipeline.update"], byActor.Entries.Select(e => e.Action).ToArray());

        var byPrefix = await Facade.QueryAsync("20050505", null, "pipeline.", 100, 0);
        Assert.Equal(2, byPrefix.Total);

        var both = await Facade.QueryAsync("20050505", "alice", "pipeline.", 100, 0);
        Assert.Equal(["pipeline.update"], both.Entries.Select(e => e.Action).ToArray());

        // Exact, not prefix, on the actor: "ali" must match nobody.
        Assert.Empty((await Facade.QueryAsync("20050505", "ali", null, 100, 0)).Entries);
    }

    /// <summary>Total counts the filtered rows BEFORE paging, so a UI can page; the entries are the
    /// page.</summary>
    [Fact]
    public async Task Query_PagesWithinTheFilteredSet()
    {
        var day = AtUtc(2006, 6, 6);
        for (var i = 0; i < 5; i++)
        {
            await Facade.AppendAsync(Entry(day + i, action: $"p{i}.act"));
        }

        var page = await Facade.QueryAsync("20060606", null, null, 2, 1);

        Assert.Equal(5, page.Total);
        Assert.Equal(["p3.act", "p2.act"], page.Entries.Select(e => e.Action).ToArray());
    }

    /// <summary>A day that was never written to is an empty page, not an error and not a null — every
    /// audit UI opens on "today", which on a quiet morning is exactly this case.</summary>
    [Fact]
    public async Task AnUnwrittenDay_IsAnEmptyPage()
    {
        var page = await Facade.QueryAsync("20070707", null, null, 100, 0);

        Assert.Empty(page.Entries);
        Assert.Equal(0, page.Total);
        Assert.Equal(0, page.Truncated);
    }

    /// <summary>An entry with no timestamp lands on TODAY rather than on 1970-01-01, which is where
    /// <c>AuditKeyFor(0)</c> would put it — a permanent junk day in the index that no operator could
    /// explain. Every real caller stamps AtMs; this is the forgotten path.</summary>
    [Fact]
    public async Task AnUnstampedEntry_LandsOnToday()
    {
        var today = DateTimeOffset.UtcNow.UtcDateTime.ToString("yyyyMMdd");
        await Facade.AppendAsync(new AuditEntry { Id = "no-timestamp", Actor = "system", Action = "unstamped.act" });

        var page = await Facade.QueryAsync(today, null, "unstamped.", 100, 0);

        Assert.Equal(["unstamped.act"], page.Entries.Select(e => e.Action).ToArray());
        Assert.DoesNotContain("19700101", await Facade.GetDaysAsync());
    }
}
