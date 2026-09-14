using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.AppCore;
using StreamsForge.AppCore.Environments;
using StreamsForge.Api.Auth;

namespace StreamsForge.Api.Hubs;

/// <summary>
/// The realtime relay's client-facing surface: a caller joins the SignalR group whose frames
/// <c>StreamBridgeService</c> (Orleans) / <c>DaprStreamBridge</c> (Dapr) is already pushing.
///
/// <para><b>Plan 015 wave 3-B — what changed and why it had to.</b> The hub was gated exactly once, by
/// <c>[Authorize("Viewer")]</c> on the class, and every subscribe method checked nothing: any
/// authenticated user could join <i>any</i> pipeline's or source's group and receive its rows, no matter
/// what their entitlements said. Since the frames are the entity's data, that made the hub the way
/// around every read entitlement REST enforces. Each subscribe now asks <see cref="AccessGuard"/> for
/// the same read action the REST route serving that entity's rows asks for, at that entity, with its
/// <c>Tags</c>. The class attribute stays: it is the compatibility floor and the thing that still
/// rejects an unauthenticated connection at negotiate time.</para>
///
/// <para><b>How a refusal reaches the client, and why <see cref="HubException"/>.</b> An ordinary
/// exception thrown from a hub method is scrubbed by SignalR — the client sees "An unexpected error
/// occurred invoking 'SubscribePipeline'" and nothing else, which is the unhelpful shape this plan
/// exists to remove. <see cref="HubException"/> is the one exception type whose <i>message</i> SignalR
/// sends to the caller verbatim, with no <c>EnableDetailedErrors</c> needed, and it surfaces in the
/// browser as a rejected <c>invoke()</c> promise carrying that text. So the refusal is a HubException
/// whose message is <see cref="StreamsForge.AppCore.Access.AccessResult.Reason"/> — the same string the
/// REST 403 body carries, so an operator debugging "why is this table empty" reads the same sentence
/// whichever transport they are on. <c>web/src/realtime/hub.ts</c> already awaits the invoke for tables
/// (<c>subscribeTable().ready</c> rejects) and fires-and-forgets the rest; making those three surface
/// the message in the UI is wave 6's job, and it needs no server change when it happens.</para>
///
/// <para><b>ponytail: entitlements are checked at SUBSCRIBE time only.</b> A grant revoked while a
/// subscription is live keeps delivering frames until that connection drops — the bridge relays to a
/// group and never looks at who is in it. Ceiling stated plainly: revocation reaches REST in one
/// <c>Auth:PolicyCacheSeconds</c> and reaches an established stream only at the next reconnect (the SPA
/// re-invokes every subscribe on <c>onreconnected</c>, so it IS re-checked then) or logout. Re-authorizing
/// per message was explicitly rejected: it would put a permission evaluation on the hot path of every
/// delta batch for every connected client. Upgrade path, when the exposure matters: have the resolver
/// raise an event when the document version moves, and on that event re-run the checks for each
/// connection's remembered subscriptions, removing the groups that no longer pass — periodic and
/// per-connection, not per-frame. That needs a per-connection subscription registry the hub does not
/// keep today, which is the actual work and the reason it is not in this wave.</para>
///
/// <para><b>Unsubscribe is deliberately ungated.</b> Leaving a group takes nothing away from anybody, and
/// a caller whose entitlement was just revoked must still be able to detach cleanly — refusing an
/// unsubscribe would strand exactly the subscription we most want gone.</para>
///
/// <para><b>Plan 021 wave 2 — group names are qualified by the CONNECTION's environment, pinned
/// verbatim against <c>StreamBridgeService</c>'s publish half.</b> A hub method invocation runs over an
/// already-established WebSocket, not through <c>EnvironmentSelectionMiddleware</c>, so
/// <c>EnvironmentAmbient</c> — an <c>AsyncLocal</c> set only by that HTTP middleware — is empty here, and
/// reading it would silently group every subscriber under the default environment regardless of what
/// they asked for. What DID go through the middleware is the HTTP request that established this
/// connection, and its <see cref="Microsoft.AspNetCore.Http.HttpContext"/> stays reachable for the
/// connection's lifetime via <c>Context.GetHttpContext()</c> — so <see cref="ConnectionEnv"/> reads the
/// environment the middleware stamped onto <c>HttpContext.Items</c>
/// (<see cref="StreamsForge.Api.EnvironmentSelectionMiddleware.HttpContextItemKey"/>) rather than the
/// ambient. The <c>"metrics"</c> group stays unqualified — it names no entity, it is cluster-wide by
/// design (see <see cref="SubscribeMetrics"/>).</para>
///
/// <para><b>Plan 020 wave G — awareness follows every rule above, plus one more of its own.</b>
/// <see cref="SubscribeAwareness"/> asks <see cref="AccessGuard"/> for
/// <see cref="StreamsForge.Abstractions.Actions.SourceRead"/> at the SAME scope
/// <see cref="SubscribeSource"/> already asks for that source — presence reveals who is working on which
/// document, so it is gated exactly like reading the document's own rows is. It ALSO refuses — visibly,
/// with a <see cref="HubException"/>, never a silent empty group — when the named source does not exist,
/// is not <see cref="StreamsForge.Abstractions.SourceKinds.Crdt"/>-kind, or has no
/// <see cref="StreamsForge.Abstractions.CrdtAwarenessConfig"/> configured, which is a deliberate departure
/// from <see cref="SubscribeSource"/>/<see cref="SubscribeTable"/>'s own "subscribing before the entity
/// exists has always been legal here" tolerance: those groups eventually receive real frames once the
/// entity exists, but a group nothing will EVER publish to (awareness off, or off by default and never
/// turned on) is a caller misconfiguration worth surfacing immediately rather than a race to tolerate.
/// State lives in <see cref="AwarenessRegistry"/>, a host-process singleton resolved per call from
/// <paramref name="services"/> exactly like <see cref="ReadCatalogAsync{T}"/> resolves
/// <see cref="ICatalogFacade"/> — see that registry's own class doc for the TTL/cap mechanics and the
/// per-host scope this inherits from SignalR having no configured backplane anywhere in this
/// platform.</para>
/// </summary>
[Authorize(Policy = "Viewer")]
public sealed class StreamHub(AccessGuard guard, IServiceProvider services, IEntityStreamFacade streams) : Hub
{
    /// <summary>The environment this connection selected, read off the HTTP request that established it
    /// — see the class remarks. A connection whose <c>HttpContext</c> somehow never reached
    /// <c>EnvironmentSelectionMiddleware</c> (there is no route today that maps the hub without it) falls
    /// back to <see cref="EnvKeys.Default"/> rather than throwing, so a subscribe request degrades to
    /// today's behaviour instead of failing outright.</summary>
    private string ConnectionEnv =>
        Context.GetHttpContext()?.Items[StreamsForge.Api.EnvironmentSelectionMiddleware.HttpContextItemKey] as string
        ?? EnvKeys.Default;

    /// <summary>Reads the catalog of THIS CONNECTION's environment. It takes an
    /// <see cref="IServiceProvider"/> rather than an injected <see cref="ICatalogFacade"/> for a reason
    /// that is easy to get wrong: <c>ICatalogFacade</c> is registered Transient and resolves the
    /// environment from <see cref="EnvironmentAmbient"/> AT RESOLUTION TIME, and a hub is activated
    /// outside any HTTP request — so a constructor-injected facade is bound to the DEFAULT environment
    /// forever, for every connection, whatever environment that connection selected. The group names
    /// would still be right (they come from <see cref="ConnectionEnv"/>), but the entitlement check would
    /// be run against the wrong environment's tags: a <c>tag:finance</c> grant would be tested against
    /// whatever entity of that name happens to live in <c>default</c>. So the facade is resolved per
    /// call, inside the connection's environment.
    ///
    /// <para><see cref="EnvironmentAmbient.WithAsync"/> is the sanctioned non-middleware writer — the
    /// case its own doc comment describes, "code that has read an entity's environment and wants the
    /// facades it calls to agree". For the default environment this is byte-identical to the old
    /// behaviour: the ambient is already default, and the same facade comes back.</para></summary>
    private async Task<T> ReadCatalogAsync<T>(Func<ICatalogFacade, Task<T>> read)
    {
        var result = default(T)!;
        await EnvironmentAmbient.WithAsync(ConnectionEnv, async () =>
            result = await read(services.GetRequiredService<ICatalogFacade>()));
        return result;
    }

    public async Task SubscribePipeline(string id)
    {
        // Tags are read best-effort: subscribing before the entity exists has always been legal here
        // (the group simply stays silent), so a miss checks with no tags rather than becoming an error.
        await EnsureAsync(Actions.PipelineRead, id, (await ReadCatalogAsync(c => c.GetPipelineAsync(id)))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"pipeline:{EnvKeys.Qualify(ConnectionEnv, id)}");
    }

    public Task UnsubscribePipeline(string id) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"pipeline:{EnvKeys.Qualify(ConnectionEnv, id)}");

    /// <summary>Plan 026 wave 2 — <see cref="SubscribePipeline"/>'s replaying twin, mirroring
    /// <see cref="SubscribeSourceFrom"/>'s shape exactly (new method name for the same reason: SignalR
    /// hubs cannot overload). Retained result batches matching <paramref name="fromSeq"/>/
    /// <paramref name="fromTimestampMs"/> (seq wins; neither set means live only) are sent to
    /// <em>this caller only</em> as the SAME <c>pipelineResult(pipelineId, rows)</c> shape
    /// <c>StreamBridgeService</c>/<c>DaprStreamBridge</c> relay to the <c>pipeline:</c> group, plus a
    /// trailing <c>position</c> argument (the batch's producer position — every row in <c>rows</c> shares
    /// it, same as gRPC's <see cref="StreamsForge.Host.Grpc.V1.ResultEnvelope.Position"/>). Once the facade
    /// call returns, this connection stops receiving batches directly and joins the ordinary
    /// <c>pipeline:</c> group instead. The hand-off is best-effort — see <see cref="SubscribeSourceFrom"/>'s
    /// own note, identical here.</summary>
    public async Task SubscribePipelineFrom(string id, long? fromSeq, long? fromTimestampMs)
    {
        var subscribed = await ReadCatalogAsync(c => c.GetPipelineAsync(id));
        await EnsureAsync(Actions.PipelineRead, id, subscribed?.Tags);

        var pipelineId = subscribed?.Id ?? id;
        var environment = subscribed?.Environment ?? ConnectionEnv;

        ReplayFrom? from = fromSeq is { } seq
            ? new ReplayFrom { Seq = seq }
            : fromTimestampMs is { } ts
                ? new ReplayFrom { TimestampMs = ts }
                : null;

        var replaying = true;
        IEntityReplaySubscription handle;
        try
        {
            handle = await streams.SubscribePipelineAsync(environment, pipelineId, from, async (rows, position) =>
            {
                if (!replaying)
                {
                    return;
                }
                await Clients.Caller.SendAsync("pipelineResult", id, rows, position);
            });
        }
        catch (NotSupportedException ex)
        {
            throw new HubException(ex.Message);
        }

        replaying = false;
        await handle.DisposeAsync();
        await Groups.AddToGroupAsync(Context.ConnectionId, $"pipeline:{EnvKeys.Qualify(ConnectionEnv, id)}");
    }

    public async Task SubscribeSource(string name)
    {
        await EnsureAsync(Actions.SourceRead, name, (await ReadCatalogAsync(c => c.GetSourceAsync(name)))?.Tags);
        await Groups.AddToGroupAsync(Context.ConnectionId, $"source:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    public Task UnsubscribeSource(string name) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"source:{EnvKeys.Qualify(ConnectionEnv, name)}");

    /// <summary>Plan 026 wave 1 — <see cref="SubscribeSource"/>'s replaying twin. A NEW method name because
    /// SignalR hubs cannot overload: a second <c>SubscribeSource</c> taking extra parameters would silently
    /// shadow rather than overload. <paramref name="fromSeq"/>/<paramref name="fromTimestampMs"/> mirror
    /// gRPC's <c>from_seq</c>/<c>from_timestamp_ms</c> (seq wins when both are set; neither set means "live
    /// only", same as calling <see cref="SubscribeSource"/>).
    ///
    /// <para>Retained events matching the request are sent to <em>this caller only</em> — not the group,
    /// which nobody but this connection asked to replay — as the SAME <c>sourceEvent</c> shape
    /// <c>StreamBridgeService</c>/<c>DaprStreamBridge</c> relay to the <c>source:</c> group (bare source
    /// name, the row, unchanged), plus a third argument carrying the producer <c>position</c>. Once the
    /// facade call returns — which happens only after every retained event has been sent (see
    /// <see cref="IEntityStreamFacade.SubscribeSourceAsync(string, string, ReplayFrom?, Func{IReadOnlyDictionary{string, object?}, long, long, Task})"/>'s
    /// own contract) — this connection stops being sent events directly and joins the ordinary
    /// <c>source:</c> group instead, exactly like <see cref="SubscribeSource"/> does, so it keeps receiving
    /// live rows for as long as it stays subscribed.</para>
    ///
    /// <para><b>The hand-off is best-effort, not exact.</b> There is a window between disposing this
    /// call's own subscription and joining the group during which a live row published by the source is
    /// relayed to nobody — the bridge's group-based relay was not joined yet, and this call's direct
    /// subscription was already torn down. Closing it would need joining the group BEFORE disposing (which
    /// risks the opposite: the same row delivered twice, once directly and once through the group) or a
    /// sequenced hand-off the bridge does not support today. gRPC's <c>SubscribeSource</c> has no such gap
    /// — one subscription serves the whole call, replay and live alike — so a caller that cannot tolerate
    /// the gap should prefer that transport. <c>// ponytail: bridge-side positions (a `position` field on
    /// the ordinary group relay, not just this replay call) are wave 4 (console) work.</c></para></summary>
    public async Task SubscribeSourceFrom(string name, long? fromSeq, long? fromTimestampMs)
    {
        await EnsureAsync(Actions.SourceRead, name, (await ReadCatalogAsync(c => c.GetSourceAsync(name)))?.Tags);

        ReplayFrom? from = fromSeq is { } seq
            ? new ReplayFrom { Seq = seq }
            : fromTimestampMs is { } ts
                ? new ReplayFrom { TimestampMs = ts }
                : null;

        var replaying = true;
        IEntityReplaySubscription handle;
        try
        {
            handle = await streams.SubscribeSourceAsync(ConnectionEnv, name, from, async (row, tsMs, position) =>
            {
                if (!replaying)
                {
                    return;
                }
                await Clients.Caller.SendAsync("sourceEvent", name, row, position);
            });
        }
        catch (NotSupportedException ex)
        {
            throw new HubException(ex.Message);
        }

        replaying = false;
        await handle.DisposeAsync();
        await Groups.AddToGroupAsync(Context.ConnectionId, $"source:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    /// <summary>The metrics group is cluster-wide and names no entity, so it asks
    /// <see cref="Actions.CatalogRead"/> at <c>*</c> — the action the wave-1 equivalence matrix already
    /// assigns to this hub and to the other platform-wide read routes (<c>/api/meta/*</c>,
    /// <c>/api/transports</c>).</summary>
    public async Task SubscribeMetrics()
    {
        await EnsureAsync(Actions.CatalogRead, "*", null);
        // Plan 021 wave 2, DELIBERATELY NOT QUALIFIED, and this is a stated limitation rather than an
        // oversight. The frames this group carries are per-pipeline throughput, so an unqualified group
        // does hand every environment's live rates to every connection holding catalog.read at *. It is
        // left cluster-wide because qualifying it correctly means the group name, BOTH flavours' bridge
        // publishers, and the payload's own PipelineId all have to move together — the payload id must
        // stay bare, since every client keys its metrics state on it — and a half-qualified metrics
        // stream is silent, not loud. The exposure is also strictly smaller than what D9 already grants:
        // any authenticated caller may address any environment through the API outright, so this leaks
        // less than the REST surface does by design. The console filters metrics to its own
        // environment's pipeline list (web/src/pages/DashboardPage.tsx); a second client would have to do
        // the same until this is qualified properly.
        await Groups.AddToGroupAsync(Context.ConnectionId, "metrics");
    }

    /// <summary>Table deltas are grouped by NAME (the delta stream's key), so whatever the caller passed
    /// has to become a name before it can name a group.
    ///
    /// <para>Plan 016 wave 1: this accepts an id or a name, like every read surface now does. It was
    /// left name-only when the REST and gRPC sites were migrated because this file belonged to no agent
    /// that wave — and the result was that gRPC's <c>SubscribeTable</c> took an id while this one did
    /// not, for the same subscription. Two transports disagreeing about what addresses an entity is the
    /// exact divergence the shared resolver exists to prevent, so it is closed here rather than
    /// recorded.</para>
    ///
    /// <para><b>The group is keyed on the RESOLVED name, never on what was passed</b> — an id-addressed
    /// subscriber has to land in the same group as a name-addressed one, or it silently receives
    /// nothing. Unresolvable input keeps its pre-existing behaviour: the entitlement is checked at the
    /// raw string and the caller joins a group nothing publishes to, which is what subscribing to a
    /// table that does not exist has always done here. An AMBIGUOUS name is refused, because there is no
    /// honest group to join — but only after the guard has run, so an unentitled caller learns
    /// nothing about which entities exist.</para></summary>
    public async Task SubscribeTable(string idOrName)
    {
        var hit = EntityRef.Resolve(await ReadCatalogAsync(c => c.GetTablesAsync()), idOrName);
        var name = hit.Value?.Name ?? idOrName;

        await EnsureAsync(Actions.TableRead, name, hit.Value?.Tags);

        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new HubException(hit.Message);
        }

        await Groups.AddToGroupAsync(Context.ConnectionId, $"table:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    public Task UnsubscribeTable(string name) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, $"table:{EnvKeys.Qualify(ConnectionEnv, name)}");

    /// <summary>Plan 026 wave 2 — <see cref="SubscribeTable"/>'s replaying twin, mirroring
    /// <see cref="SubscribeSourceFrom"/>'s shape (new method name; SignalR hubs cannot overload). Same
    /// id-or-name resolution <see cref="SubscribeTable"/> does — the group and the direct sends both use
    /// the RESOLVED name. Retained delta batches matching <paramref name="fromSeq"/>/
    /// <paramref name="fromTimestampMs"/> (seq wins; neither set means live only) are sent to
    /// <em>this caller only</em> as the SAME <c>tableDelta(tableName, deltas, seq)</c> shape
    /// <c>StreamBridgeService</c>/<c>DaprStreamBridge</c> relay to the <c>table:</c> group — <c>seq</c> here
    /// is this call's own per-subscription batch counter, not the bridge's netted-flush one — plus a
    /// trailing <c>position</c> argument (the batch's producer position). Once the facade call returns,
    /// this connection joins the ordinary <c>table:</c> group. The hand-off is best-effort — see
    /// <see cref="SubscribeSourceFrom"/>'s own note, identical here.</summary>
    public async Task SubscribeTableFrom(string idOrName, long? fromSeq, long? fromTimestampMs)
    {
        var hit = EntityRef.Resolve(await ReadCatalogAsync(c => c.GetTablesAsync()), idOrName);
        var name = hit.Value?.Name ?? idOrName;

        await EnsureAsync(Actions.TableRead, name, hit.Value?.Tags);

        if (hit.Outcome == EntityRefOutcome.Ambiguous)
        {
            throw new HubException(hit.Message);
        }

        var environment = hit.Value?.Environment ?? ConnectionEnv;

        ReplayFrom? from = fromSeq is { } seq
            ? new ReplayFrom { Seq = seq }
            : fromTimestampMs is { } ts
                ? new ReplayFrom { TimestampMs = ts }
                : null;

        var replaying = true;
        long batchSeq = 0;
        IEntityReplaySubscription handle;
        try
        {
            handle = await streams.SubscribeTableAsync(environment, name, from, async (deltas, position) =>
            {
                if (!replaying)
                {
                    return;
                }
                await Clients.Caller.SendAsync("tableDelta", name, deltas, ++batchSeq, position);
            });
        }
        catch (NotSupportedException ex)
        {
            throw new HubException(ex.Message);
        }

        replaying = false;
        await handle.DisposeAsync();
        await Groups.AddToGroupAsync(Context.ConnectionId, $"table:{EnvKeys.Qualify(ConnectionEnv, name)}");
    }

    // -------------------------------------------------------------------------------------------------
    // Plan 020 wave G — awareness. See this class's own remarks for the authorization rule and
    // AwarenessRegistry's for the TTL/cap mechanics and the per-host scope.
    // -------------------------------------------------------------------------------------------------

    private string AwarenessGroup(string sourceName) => $"crdt-awareness:{EnvKeys.Qualify(ConnectionEnv, sourceName)}";

    /// <summary>Joins this connection's presence entry to <paramref name="sourceName"/>'s awareness group
    /// and returns the current membership (including this entry) plus the two numbers
    /// <see cref="Heartbeat"/>'s caller needs to behave itself. <paramref name="clientId"/> distinguishes
    /// two tabs/connections from the same authenticated identity; <paramref name="label"/> is arbitrary
    /// client-chosen cosmetic detail (a cursor color, a display variant) — see
    /// <see cref="AwarenessEntry"/>'s own doc comment for why neither is trusted as the identity itself.
    ///
    /// <para>Refuses (never a silent empty group) when: the guard denies
    /// <see cref="Actions.SourceRead"/> at this source; the source does not exist; the source is not
    /// <see cref="SourceKinds.Crdt"/>-kind; the source has no <see cref="CrdtAwarenessConfig"/> (awareness
    /// is off — the default); or the document is already at its configured cap and this connection is not
    /// already a member of it.</para></summary>
    public async Task<AwarenessSnapshot> SubscribeAwareness(string sourceName, string clientId, string? label)
    {
        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new HubException("clientId is required");
        }

        var src = await ReadCatalogAsync(c => c.GetSourceAsync(sourceName));
        await EnsureAsync(Actions.SourceRead, src?.Name ?? sourceName, src?.Tags);

        if (src is null)
        {
            throw new HubException($"source '{sourceName}' not found");
        }
        if (src.Kind != SourceKinds.Crdt)
        {
            throw new HubException($"source '{sourceName}' is not crdt-kind");
        }
        var awareness = src.Connector?.Crdt?.Awareness;
        if (awareness is null)
        {
            throw new HubException($"awareness is not enabled for source '{sourceName}' (CrdtSourceConfig.Awareness is unset)");
        }

        var registry = services.GetRequiredService<AwarenessRegistry>();
        var group = AwarenessGroup(src.Name);
        var identity = Context.User?.Identity?.Name ?? "(anonymous)";
        var ttl = TimeSpan.FromSeconds(Math.Max(1, awareness.TtlSeconds));

        var joined = registry.Join(group, Context.ConnectionId, clientId, identity, label, ttl, awareness.MaxEntries);
        if (!joined.Ok)
        {
            throw new HubException(joined.Reason!);
        }

        await Groups.AddToGroupAsync(Context.ConnectionId, group);
        await Clients.OthersInGroup(group).SendAsync("awarenessUpdate", src.Name, joined.Peers);

        return new AwarenessSnapshot(awareness.TtlSeconds, awareness.MaxEntries, joined.Peers);
    }

    /// <summary>Refreshes this connection's own presence entry. Deliberately UNGATED — see
    /// <see cref="UnsubscribeTable"/>'s own precedent and <see cref="AwarenessRegistry.Heartbeat"/>'s doc
    /// comment for why that is safe: it can only refresh an entry <see cref="SubscribeAwareness"/> already
    /// created under a guard check, never create one itself. Broadcasts the refreshed membership to the
    /// group only when this call's own eviction pass actually removed a stale peer — an ordinary heartbeat
    /// that changes nothing observable sends nothing, which is what keeps steady-state traffic bounded to
    /// roughly one small message per heartbeat interval per member instead of one per member per
    /// member.</summary>
    public async Task Heartbeat(string sourceName)
    {
        var registry = services.GetRequiredService<AwarenessRegistry>();
        var group = AwarenessGroup(sourceName);
        var result = registry.Heartbeat(group, Context.ConnectionId);
        if (result.MembershipChanged)
        {
            await Clients.Group(group).SendAsync("awarenessUpdate", sourceName, result.Peers);
        }
    }

    /// <summary>Leaves this connection's presence entry. Ungated for the same reason
    /// <see cref="UnsubscribeTable"/> is: leaving takes nothing away from anybody.</summary>
    public async Task UnsubscribeAwareness(string sourceName)
    {
        var registry = services.GetRequiredService<AwarenessRegistry>();
        var group = AwarenessGroup(sourceName);
        var peers = registry.Leave(group, Context.ConnectionId);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
        if (peers is not null)
        {
            await Clients.Group(group).SendAsync("awarenessUpdate", sourceName, peers);
        }
    }

    /// <summary>A dropped connection never gets to call <see cref="UnsubscribeAwareness"/> for whatever it
    /// had joined, so this is where that cleanup happens instead — for every awareness document this
    /// connection was a member of, not just one.</summary>
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var registry = services.GetRequiredService<AwarenessRegistry>();
        foreach (var (group, sourceName, peers) in registry.RemoveConnection(Context.ConnectionId))
        {
            await Clients.Group(group).SendAsync("awarenessUpdate", sourceName, peers);
        }
        await base.OnDisconnectedAsync(exception);
    }

    /// <summary>Returns normally when the caller may subscribe; throws the one exception type SignalR
    /// relays verbatim otherwise.
    ///
    /// <para>A <see cref="AccessDecision.RequiresApproval"/> answer is refused too, and says so in its own
    /// words ("grant … requires approval") — it is NOT a denial, but filing the request is waves 4-5's
    /// job and no machinery exists yet. "Approve a live stream subscription" is also a shape worth
    /// thinking about before building, rather than falling into. Refusing fails closed in the meantime,
    /// and the message tells the caller which of the two happened.</para></summary>
    private async Task EnsureAsync(string action, string scope, IReadOnlyCollection<string>? tags)
    {
        var result = await guard.CheckAsync(Context.User ?? new System.Security.Claims.ClaimsPrincipal(), action, scope, tags);
        if (!result.IsAllowed)
        {
            throw new HubException(result.Reason);
        }
    }
}
