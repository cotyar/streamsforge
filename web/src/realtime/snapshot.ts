import type { ResultEnvelope } from '../api/types'

/** Read despite a stalled hub, then reconcile again if its subscription eventually succeeds. */
export function refreshAfterSubscription(
  ready: Promise<void>,
  refresh: () => void,
  fallbackMs = 3000,
): () => void {
  let cancelled = false
  const timer = setTimeout(() => { if (!cancelled) refresh() }, fallbackMs)
  void ready.catch(() => {}).then(() => {
    clearTimeout(timer)
    if (!cancelled) refresh()
  })
  return () => {
    cancelled = true
    clearTimeout(timer)
  }
}

/** Merge a retained REST buffer without losing rows delivered live during its request. */
export function mergePipelineResults(
  current: readonly ResultEnvelope[],
  snapshot: readonly ResultEnvelope[],
  limit: number,
): ResultEnvelope[] {
  const bySeq = new Map(current.map((row) => [row.seq, row]))
  for (const row of snapshot) bySeq.set(row.seq, row)
  return [...bySeq.values()].sort((a, b) => b.seq - a.seq).slice(0, limit)
}
