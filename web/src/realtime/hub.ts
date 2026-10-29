// Singleton SignalR connection manager for the /hubs/stream hub.
// Connects lazily — the first subscribe*() call after login triggers connection
// setup; accessTokenFactory reads the current bearer token from localStorage on
// every (re)connect attempt, so no explicit wiring to the auth module is needed.
import * as signalR from '@microsoft/signalr'
import type { PipelineMetrics, PipelineStatus, ResultEnvelope, ResultRow, TableRowDto } from '../api/types'
import { getStoredToken } from '../api/client'
import { getStoredEnvironment, needsEnvironmentSelector } from '../lib/environment'

type RowsHandler = (rows: ResultEnvelope[]) => void
type StatusHandler = (status: PipelineStatus) => void
type MetricsHandler = (metrics: PipelineMetrics) => void
type SourceHandler = (row: ResultRow) => void
type TableDeltaHandler = (deltas: TableRowDto[], seq: number) => void
type Unsubscribe = () => void
/** A callable cleanup with a current-connection subscription acknowledgement. Readers
 * can await `.ready` before taking a snapshot; callers needing only deltas use the
 * cleanup as before. Reconnect re-arms the getter for both pipelines and tables. */
type ReadyUnsubscribe = Unsubscribe & { ready: Promise<void> }

let connection: signalR.HubConnection | null = null
let connectPromise: Promise<signalR.HubConnection> | null = null

const pipelineRowHandlers = new Map<string, Set<RowsHandler>>()
const pipelineStatusHandlers = new Map<string, Set<StatusHandler>>()
const pipelineRefCounts = new Map<string, number>()
const pipelineReadyPromises = new Map<string, Promise<void>>()

const sourceHandlers = new Map<string, Set<SourceHandler>>()
const sourceRefCounts = new Map<string, number>()

const reconnectHandlers = new Set<() => void>()

/** Recover snapshots after reconnect starts restoring groups; table readers await their own ready. */
export function onHubReconnected(handler: () => void): Unsubscribe {
  reconnectHandlers.add(handler)
  return () => { reconnectHandlers.delete(handler) }
}

const metricsHandlers = new Set<MetricsHandler>()
let metricsRefCount = 0

const tableDeltaHandlers = new Map<string, Set<TableDeltaHandler>>()
const tableRefCounts = new Map<string, number>()
/** Per-table-name promise tracking the in-flight/most-recent `SubscribeTable` invoke on the
 * CURRENT connection -- see subscribeTable() and the onreconnected() handler below, which is the
 * only other writer (it re-arms this on every reconnect for every table still referenced). */
const tableReadyPromises = new Map<string, Promise<void>>()

function registerListeners(conn: signalR.HubConnection) {
  conn.on('pipelineResult', (pipelineId: string, rows: ResultEnvelope[]) => {
    pipelineRowHandlers.get(pipelineId)?.forEach((h) => h(rows))
  })
  conn.on('pipelineStatus', (pipelineId: string, status: PipelineStatus) => {
    pipelineStatusHandlers.get(pipelineId)?.forEach((h) => h(status))
  })
  conn.on('pipelineMetrics', (metrics: PipelineMetrics) => {
    metricsHandlers.forEach((h) => h(metrics))
  })
  conn.on('sourceEvent', (sourceName: string, row: ResultRow) => {
    sourceHandlers.get(sourceName)?.forEach((h) => h(row))
  })
  conn.on('tableDelta', (tableName: string, deltas: TableRowDto[], seq: number) => {
    tableDeltaHandlers.get(tableName)?.forEach((h) => h(deltas, seq))
  })

  conn.onreconnected(() => {
    for (const id of pipelineRefCounts.keys()) {
      const rearmed = conn.invoke('subscribePipeline', id)
      rearmed.catch(() => {})
      pipelineReadyPromises.set(id, rearmed)
    }
    for (const name of sourceRefCounts.keys()) {
      void conn.invoke('subscribeSource', name).catch(() => {})
    }
    if (metricsRefCount > 0) {
      void conn.invoke('subscribeMetrics').catch(() => {})
    }
    // Re-arms tableReadyPromises too (not just fire-and-forget), so a `.ready` read shortly after
    // a reconnect reflects the CURRENT connection's registration rather than a stale promise that
    // resolved against a connection that has since dropped.
    for (const name of tableRefCounts.keys()) {
      const rearmed = conn.invoke('SubscribeTable', name)
      rearmed.catch(() => {
        // Handled here too (see subscribeTable()'s identical comment) purely so an unread
        // rejection doesn't surface as an unhandled-rejection warning; `.ready` readers still see
        // the real rejection via `rearmed` itself.
      })
      tableReadyPromises.set(name, rearmed)
    }
    reconnectHandlers.forEach((handler) => handler())
  })
}

/**
 * Transport override for hostile networks (corporate proxies that kill the WebSocket upgrade
 * mid-handshake, hanging SignalR's own auto-fallback). Set once via URL query — ?transport=lp
 * (LongPolling), ?transport=sse (ServerSentEvents), ?transport=ws (WebSockets only),
 * ?transport=auto (clear) — persisted in localStorage ('sf.transport') so every page and later
 * visits keep it. Unset = SignalR's stock negotiation (WS → SSE → LP).
 */
// Query → localStorage sync must run at MODULE LOAD: the router rewrites the URL (login
// redirect) before the first hub connection, so reading location.search lazily misses it.
try {
  const fromUrl = new URLSearchParams(window.location.search).get('transport')
  if (fromUrl !== null) {
    if (fromUrl === 'auto' || fromUrl === '') localStorage.removeItem('sf.transport')
    else localStorage.setItem('sf.transport', fromUrl)
  }
} catch {
  /* ignore (SSR/tests) */
}

function resolveTransport(): signalR.HttpTransportType | undefined {
  try {
    switch (localStorage.getItem('sf.transport')) {
      case 'lp':
        return signalR.HttpTransportType.LongPolling
      case 'sse':
        return signalR.HttpTransportType.ServerSentEvents
      case 'ws':
        return signalR.HttpTransportType.WebSockets
      default:
        return undefined
    }
  } catch {
    return undefined
  }
}

/** Plan 021 wave 2 (021-F): a WebSocket/SSE connection cannot carry the `X-StreamsForge-Environment`
 * header (see client.ts), so the hub takes `?env=<name>` instead — the same override
 * EnvironmentSelectionMiddleware.cs documents for "a browser navigation or any other caller that
 * cannot set a header on the request that matters", and the one the middleware stamps onto
 * HttpContext.Items for StreamHub to read (the negotiate/connect request IS an HTTP request, so it
 * still goes through the middleware even though the long-lived connection built on top of it does
 * not). Read fresh on every (re)connect, not cached, so a switch that tears the connection down (see
 * EnvironmentPicker's switchTo()) reconnects against whichever environment is current at that moment. */
function hubUrl(): string {
  const env = getStoredEnvironment()
  return needsEnvironmentSelector(env) ? `/hubs/stream?env=${encodeURIComponent(env)}` : '/hubs/stream'
}

function getConnection(): Promise<signalR.HubConnection> {
  if (connection) return Promise.resolve(connection)
  if (connectPromise) return connectPromise

  const forcedTransport = resolveTransport()
  if (forcedTransport !== undefined) {
    console.info(`[hub] SignalR transport forced via sf.transport: ${signalR.HttpTransportType[forcedTransport]}`)
  }

  const conn = new signalR.HubConnectionBuilder()
    .withUrl(hubUrl(), {
      accessTokenFactory: () => getStoredToken() ?? '',
      ...(forcedTransport !== undefined ? { transport: forcedTransport } : {}),
    })
    // Retry forever (default gives up after 4 attempts, leaving a stale tab after a server
    // restart): exponential backoff capped at 15s. onreconnected() re-subscribes everything.
    .withAutomaticReconnect({
      nextRetryDelayInMilliseconds: (ctx) => Math.min(15_000, 1000 * 2 ** ctx.previousRetryCount),
    })
    .build()

  registerListeners(conn)

  connectPromise = conn
    .start()
    .then(() => {
      connection = conn
      connectPromise = null
      return conn
    })
    .catch((err) => {
      connectPromise = null
      throw err
    })

  return connectPromise
}

/** Stops the connection (if any) and clears all subscription state. Call on logout, and — plan 021
 * wave 2 — call on an environment switch too: `connection = null` runs synchronously (before the
 * `await conn.stop()`), so by the time this call returns to its caller the next getConnection() is
 * already guaranteed to build a brand new connection against hubUrl()'s then-current environment,
 * rather than reusing one negotiated against the environment being left. EnvironmentPicker.tsx calls
 * this and only then persists the new selection (setStoredEnvironment), so hubUrl() reads the new name
 * on that fresh connection. Subscribers that unmount as part of the same switch (TablesPage etc.,
 * remounted via Layout keying its routed content on the environment) see `connection` already null in
 * their cleanup and skip the now-pointless `invoke('Unsubscribe...')` — no error, just a no-op. */
export async function disconnectHub(): Promise<void> {
  const conn = connection
  connection = null
  connectPromise = null
  pipelineRowHandlers.clear()
  pipelineStatusHandlers.clear()
  pipelineRefCounts.clear()
  pipelineReadyPromises.clear()
  sourceHandlers.clear()
  sourceRefCounts.clear()
  reconnectHandlers.clear()
  metricsHandlers.clear()
  metricsRefCount = 0
  tableDeltaHandlers.clear()
  tableRefCounts.clear()
  tableReadyPromises.clear()
  if (conn) {
    try {
      await conn.stop()
    } catch {
      // ignore — connection is being torn down anyway
    }
  }
}

export function subscribePipeline(id: string, onRows: RowsHandler, onStatus?: StatusHandler): ReadyUnsubscribe {
  if (!pipelineRowHandlers.has(id)) pipelineRowHandlers.set(id, new Set())
  pipelineRowHandlers.get(id)!.add(onRows)
  if (onStatus) {
    if (!pipelineStatusHandlers.has(id)) pipelineStatusHandlers.set(id, new Set())
    pipelineStatusHandlers.get(id)!.add(onStatus)
  }

  const wasZero = (pipelineRefCounts.get(id) ?? 0) === 0
  pipelineRefCounts.set(id, (pipelineRefCounts.get(id) ?? 0) + 1)

  let ready: Promise<void>
  if (wasZero) {
    ready = getConnection().then(async (conn) => {
      if (pipelineRowHandlers.get(id)?.has(onRows)) await conn.invoke('subscribePipeline', id)
    })
    ready.catch(() => {})
    pipelineReadyPromises.set(id, ready)
  } else {
    ready = pipelineReadyPromises.get(id) ?? Promise.resolve()
  }

  let done = false
  const unsubscribe = (() => {
    if (done) return
    done = true
    pipelineRowHandlers.get(id)?.delete(onRows)
    if (onStatus) pipelineStatusHandlers.get(id)?.delete(onStatus)
    const next = Math.max(0, (pipelineRefCounts.get(id) ?? 1) - 1)
    if (next === 0) {
      pipelineRefCounts.delete(id)
      pipelineReadyPromises.delete(id)
      pipelineRowHandlers.delete(id)
      pipelineStatusHandlers.delete(id)
      if (connection) void connection.invoke('unsubscribePipeline', id)
    } else {
      pipelineRefCounts.set(id, next)
    }
  }) as ReadyUnsubscribe
  Object.defineProperty(unsubscribe, 'ready', {
    get: () => pipelineReadyPromises.get(id) ?? ready,
  })
  return unsubscribe
}

export function subscribeSource(name: string, onEvent: SourceHandler): Unsubscribe {
  if (!sourceHandlers.has(name)) sourceHandlers.set(name, new Set())
  sourceHandlers.get(name)!.add(onEvent)

  const wasZero = (sourceRefCounts.get(name) ?? 0) === 0
  sourceRefCounts.set(name, (sourceRefCounts.get(name) ?? 0) + 1)

  if (wasZero) {
    void getConnection().then((conn) => {
      if ((sourceRefCounts.get(name) ?? 0) > 0) {
        void conn.invoke('subscribeSource', name)
      }
    })
  }

  let done = false
  return () => {
    if (done) return
    done = true
    sourceHandlers.get(name)?.delete(onEvent)
    const next = Math.max(0, (sourceRefCounts.get(name) ?? 1) - 1)
    if (next === 0) {
      sourceRefCounts.delete(name)
      sourceHandlers.delete(name)
      if (connection) void connection.invoke('unsubscribeSource', name)
    } else {
      sourceRefCounts.set(name, next)
    }
  }
}

/** Subscribes to a materialized table's live delta stream by name (tables share the sources'
 * namespace, so the hub keys off name rather than id — matches `SubscribeTable`/`UnsubscribeTable`
 * on the hub, confirmed empirically to be the exact invoke-method casing the server expects).
 *
 * Gates the deferred `invoke('SubscribeTable', ...)` on *this handler* still being registered
 * (rather than on the ref count being >0) so that a subscribe immediately followed by an
 * unsubscribe — e.g. React StrictMode's dev-only mount→cleanup→remount — can't race the
 * connection handshake into double-subscribing the server. With a ref-count-only gate, the first
 * mount's orphaned `.then()` would still see a >0 count (bumped by the second mount) and send its
 * own redundant SubscribeTable, which duplicated every delta batch server-side.
 *
 * The returned function also carries `.ready`: a promise that resolves once `SubscribeTable` has
 * actually been confirmed by the server on the current connection (rejects if that invocation
 * fails). `StreamHub.SubscribeTable` on the server returns `Groups.AddToGroupAsync(...)` itself,
 * so SignalR only completes this invoke once the connection is genuinely in the table's broadcast
 * group — awaiting it is a hard guarantee, not a heuristic. This closes the lost-update race: if a
 * consumer (useTableRows.ts) issues its GET /rows snapshot read before this resolves, a delta
 * broadcast in that window is never sent to this connection at all (no backfill on a later
 * subscribe either) — a LATEST BY table then silently drops that row until something else touches
 * it. The `tableDelta` listener itself is registered synchronously in registerListeners(), before
 * connect/invoke ever happen, so nothing arriving after registration is ever missed — only the
 * window before registration completes was the gap. `.ready` is additive: every existing caller
 * that doesn't read it (ApiExplorerPage.tsx) keeps using the return value as a plain callable. */
export function subscribeTable(name: string, onDeltas: TableDeltaHandler): ReadyUnsubscribe {
  if (!tableDeltaHandlers.has(name)) tableDeltaHandlers.set(name, new Set())
  tableDeltaHandlers.get(name)!.add(onDeltas)

  const wasZero = (tableRefCounts.get(name) ?? 0) === 0
  tableRefCounts.set(name, (tableRefCounts.get(name) ?? 0) + 1)

  let ready: Promise<void>
  if (wasZero) {
    ready = getConnection().then(async (conn) => {
      // Same StrictMode guard as before: only invoke if this exact handler is still the reason
      // the table is referenced. If it's gone, treat registration as a no-op success — there is
      // nothing left for `.ready` to gate for this (already-unsubscribed) caller.
      if (!tableDeltaHandlers.get(name)?.has(onDeltas)) return
      await conn.invoke('SubscribeTable', name)
    })
    ready.catch(() => {
      // Marks the promise "handled" so an unread rejection (e.g. nobody awaits `.ready`) doesn't
      // surface as an unhandled-rejection warning; callers that DO await `.ready` still observe
      // the real rejection via the `ready` reference stored below.
    })
    tableReadyPromises.set(name, ready)
  } else {
    // Another subscriber already triggered (or completed) registration for this name — reuse its
    // promise rather than invoking SubscribeTable a second time.
    ready = tableReadyPromises.get(name) ?? Promise.resolve()
  }

  let done = false
  const unsubscribe = (() => {
    if (done) return
    done = true
    tableDeltaHandlers.get(name)?.delete(onDeltas)
    const next = Math.max(0, (tableRefCounts.get(name) ?? 1) - 1)
    if (next === 0) {
      tableRefCounts.delete(name)
      tableDeltaHandlers.delete(name)
      tableReadyPromises.delete(name)
      if (connection) void connection.invoke('UnsubscribeTable', name)
    } else {
      tableRefCounts.set(name, next)
    }
  }) as ReadyUnsubscribe
  Object.defineProperty(unsubscribe, 'ready', {
    get: () => tableReadyPromises.get(name) ?? ready,
  })
  return unsubscribe
}

export function subscribeMetrics(onMetrics: MetricsHandler): Unsubscribe {
  metricsHandlers.add(onMetrics)
  const wasZero = metricsRefCount === 0
  metricsRefCount += 1

  if (wasZero) {
    void getConnection().then((conn) => {
      if (metricsRefCount > 0) void conn.invoke('subscribeMetrics')
    })
  }

  let done = false
  return () => {
    if (done) return
    done = true
    metricsHandlers.delete(onMetrics)
    metricsRefCount = Math.max(0, metricsRefCount - 1)
    // Hub contract exposes no unsubscribeMetrics — the server keeps pushing at
    // low cost; simply stop forwarding to local listeners once none remain.
  }
}
