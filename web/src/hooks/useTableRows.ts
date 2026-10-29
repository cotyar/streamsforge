import { useEffect, useRef, useState } from 'react'
import { ZSet } from '@streamsforge/client'
import type { Row as LiveTableRow } from '@streamsforge/client'
import type { ResultRow, TableRowDto } from '../api/types'
import { tablesApi } from '../api/tables'
import { onHubReconnected, subscribeTable } from '../realtime/hub'
import { refreshAfterSubscription } from '../realtime/snapshot'

export interface LiveRow {
  /** Canonical Z-set tuple identity -- see @streamsforge/client's canonicalKey(). */
  key: string
  row: ResultRow
  weight: number
}

/** FALLBACK ONLY -- wishlist #18 put a table's real row-identity key on the wire
 * (`TableDefinition.keyFields`, recomputed on every successful compile), so this console now
 * groups by that instead of guessing (see `createZSet` below). This heuristic survives purely for
 * an OLDER engine that predates the field: `Object.keys()` on a JSON.parse()'d object preserves
 * source field order, which mirrors the SELECT list, so a table's first/leading column (e.g.
 * `symbol`) is reliably its grouping column without needing output-schema metadata -- imperfect
 * (silently wrong for a composite key or a global aggregate) but a safer default than never
 * superseding at all on a table that used to work. @streamsforge/client's own default policy
 * deliberately does NOT guess a column (see its zset.ts docstring), so this is passed to `ZSet` as
 * an explicit `groupKeyFn` override -- console-specific policy layered on the package's shared
 * bookkeeping, not a fork of it. */
function firstColumnGroupKey(row: LiveTableRow): string | null {
  const firstField = Object.keys(row)[0]
  if (firstField === undefined) return null
  return `${firstField}=${JSON.stringify(row[firstField])}`
}

/** Logged at most once per table name -- an engine old enough to omit `keyFields` entirely is a
 * one-time-interesting fact about that table's connection, not something worth repeating on every
 * reconnect or re-render. */
const warnedMissingKeyFields = new Set<string>()

/** `keyFields` is `TableDefinition.keyFields` exactly as the page's own fetch parsed it, so the
 * distinction JS already makes between an absent JSON property (`undefined`) and an explicit
 * `null` carries straight through: `undefined` means THIS ENGINE NEVER ANSWERED THE QUESTION
 * (predates wishlist #18) -- the one case this hook still falls back to the leading-column
 * heuristic for, via an explicit `groupKeyFn` override, and warns about once. `null` and a
 * (possibly empty) array are both real answers FROM the engine (whole-row identity, and
 * GROUP BY/LATEST BY key columns or `[]` for a global aggregate respectively) and are passed
 * straight to `ZSet`'s own `keyFields`-based policy -- no heuristic involved, because there is
 * nothing to guess. Collapsing "didn't answer" into "answered null" here would silently turn a
 * LATEST BY table's real key into whole-row identity on an old engine and grow duplicate rows on
 * every tick -- exactly the regression wishlist #18 exists to prevent. */
function createZSet(tableName: string, keyFields: readonly string[] | null | undefined): ZSet {
  if (keyFields !== undefined) return new ZSet(keyFields)
  if (!warnedMissingKeyFields.has(tableName)) {
    warnedMissingKeyFields.add(tableName)
    console.warn(
      `streamsforge: table '${tableName}' has no keyFields on the wire (this engine build predates wishlist #18) -- ` +
        'falling back to the leading-column heuristic. Upgrade the engine to get the table\'s real row-identity key.',
    )
  }
  return new ZSet(null, firstColumnGroupKey)
}

/**
 * Live view of a materialized table's rows.
 *
 * The Z-set reducer itself (canonical-row identity, weight summation, group supersession, the
 * content-based "already reflected" replay heuristic) lives in `@streamsforge/client`'s `zset.ts`
 * now -- extracted from this hook, which used to hand-roll all of it inline. Read that module's
 * doc comment for the hazards it defends against (arrival order isn't guaranteed, `GET /rows`'s
 * `seq` and the hub's per-batch `seq` are different counters on different scales -- measured
 * ~860 vs ~15,000 at the same instant). What stays HERE, because it's genuinely this hook's own
 * concern rather than the reducer's: subscribing to the hub BEFORE the snapshot read so no delta
 * is lost to the race, buffering until the snapshot lands, replaying, and the 900ms flash-key
 * timer for the UI's "this row just changed" highlight.
 */
export function useTableRows(
  tableId: string | undefined,
  tableName: string | undefined,
  keyFields?: readonly string[] | null,
) {
  const [rows, setRows] = useState<LiveRow[]>([])
  const [snapshotTotal, setSnapshotTotal] = useState(0)
  const [live, setLive] = useState(false)
  const [loading, setLoading] = useState(true)
  const [flashKeys, setFlashKeys] = useState<Set<string>>(new Set())

  const zsetRef = useRef<ZSet>(new ZSet(null, firstColumnGroupKey))

  // `table.keyFields` is a fresh array/undefined every time the page's own fetch re-resolves
  // (SearchAndView's polling, quick-toggle edits, etc.), so depending on the array's identity
  // would reset this hook's ZSet -- dropping and resubscribing the live view -- on every
  // unrelated refetch. Serialize to a primitive so the effect below only re-keys when the
  // three-way ANSWER actually changed, not the object it arrived in.
  const keyFieldsDepKey = keyFields === undefined ? 'undefined' : keyFields === null ? 'null' : keyFields.join(' ')

  useEffect(() => {
    zsetRef.current = createZSet(tableName ?? '', keyFields)
    setRows([])
    setSnapshotTotal(0)
    setLive(false)
    setFlashKeys(new Set())

    if (!tableId || !tableName || tableId === 'new') {
      setLoading(false)
      return
    }

    setLoading(true)
    let cancelled = false
    let buffering = true
    const buffered: { deltas: TableRowDto[]; seq: number }[] = []
    let flashTimer: ReturnType<typeof setTimeout> | null = null

    function toDeltaTuples(deltas: TableRowDto[]): Array<readonly [ResultRow, number]> {
      return deltas.map((d) => [d.row, d.weight] as const)
    }

    function flushToState() {
      // @streamsforge/client's Entry.row is `Row` (Record<string, unknown>); ResultRow is the
      // narrower RowValue-typed shape this console's components expect. Every row on the wire is
      // JSON already, so this is a type-level narrowing, not a runtime conversion.
      setRows(zsetRef.current.entries() as unknown as LiveRow[])
    }

    function applyBatch(deltas: TableRowDto[], trackFlash: boolean) {
      const touched = zsetRef.current.apply(toDeltaTuples(deltas))
      if (trackFlash && touched.length > 0) {
        setFlashKeys((prev) => {
          const next = new Set(prev)
          touched.forEach((k) => next.add(k))
          return next
        })
        if (flashTimer) clearTimeout(flashTimer)
        flashTimer = setTimeout(() => {
          if (!cancelled) setFlashKeys(new Set())
        }, 900)
      }
    }

    // Subscribe via subscribeTable() BEFORE issuing the GET /rows request, buffering every
    // incoming delta batch instead of applying it -- see the reconciliation strategy in
    // @streamsforge/client's zset.ts docstring for why (a delta landing between the REST read and
    // the hub subscribe ack must be neither dropped nor double-applied).
    const unsub = subscribeTable(tableName, (deltas, seq) => {
      if (buffering) {
        buffered.push({ deltas, seq })
        return
      }
      applyBatch(deltas, true)
      flushToState()
    })

    // Prefer an acknowledged subscription before reading. Buffer deltas while each
    // snapshot is in flight, then reconcile them against the response. A reconnect
    // starts a fresh read; its generation prevents an older response replacing it.
    let refreshVersion = 0
    const refreshSnapshot = () => {
      const version = ++refreshVersion
      buffering = true
      buffered.length = 0
      return tablesApi.rows(tableId, 500)
        .then((res) => {
          if (cancelled || version !== refreshVersion || !res) return
          zsetRef.current.seed(toDeltaTuples(res.rows))
          setSnapshotTotal(res.totalRows)

          // Replay in arrival order (reliably chronological -- single SignalR connection), skipping
          // batches whose transition already happened before the snapshot was taken.
          for (const b of buffered) {
            if (!zsetRef.current.alreadyReflected(toDeltaTuples(b.deltas))) {
              applyBatch(b.deltas, false)
            }
          }

          buffering = false
          flushToState()
        })
        .catch(() => {
          // best-effort -- once buffering flips off, live deltas still populate the view
          if (!cancelled && version === refreshVersion) buffering = false
        })
        .finally(() => {
          if (!cancelled && version === refreshVersion) setLoading(false)
        })
    }

    // Reading an idle table must not depend on a WebSocket handshake finishing.
    // If registration is slow, show a REST snapshot; once registration does finish,
    // take another buffered snapshot to close the fallback's subscription gap.
    let registrationVersion = 0
    const recover = () => {
      const version = ++registrationVersion
      setLive(false)
      const ready = unsub.ready
      void ready.then(() => {
        if (!cancelled && version === registrationVersion) setLive(true)
      }).catch(() => {})
      return refreshAfterSubscription(ready, () => { void refreshSnapshot() })
    }
    let stopWaiting = recover()
    const stopRefresh = onHubReconnected(() => {
      stopWaiting()
      stopWaiting = recover()
    })

    return () => {
      cancelled = true
      if (flashTimer) clearTimeout(flashTimer)
      stopWaiting()
      stopRefresh()
      unsub()
    }
    // keyFieldsDepKey (not keyFields itself -- see its own comment above) intentionally re-keys
    // this effect only when the resolved key actually changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableId, tableName, keyFieldsDepKey])

  return { rows, snapshotTotal, live, loading, flashKeys }
}
