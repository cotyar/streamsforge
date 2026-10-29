import { expect, test } from 'bun:test'
import { mergePipelineResults, refreshAfterSubscription } from '../src/realtime/snapshot'

test('an idle snapshot is readable while subscription is stalled, and reconciled after ack', async () => {
  let acknowledge!: () => void
  const ready = new Promise<void>((resolve) => { acknowledge = resolve })
  let reads = 0
  const stop = refreshAfterSubscription(ready, () => { reads++ }, 1)
  await Bun.sleep(10)
  expect(reads).toBe(1)
  acknowledge()
  await Bun.sleep(0)
  expect(reads).toBe(2)
  stop()
})

test('normal registration reads only once', async () => {
  let reads = 0
  const stop = refreshAfterSubscription(Promise.resolve(), () => { reads++ }, 1)
  await Bun.sleep(10)
  expect(reads).toBe(1)
  stop()
})

test('failed registration still reads the idle snapshot', async () => {
  let reads = 0
  const stop = refreshAfterSubscription(Promise.reject(new Error('offline')), () => { reads++ }, 1)
  await Bun.sleep(10)
  expect(reads).toBe(1)
  stop()
})

test('unmount cancels both timeout and delayed registration', async () => {
  let acknowledge!: () => void
  const ready = new Promise<void>((resolve) => { acknowledge = resolve })
  let reads = 0
  const stop = refreshAfterSubscription(ready, () => { reads++ }, 1)
  stop()
  acknowledge()
  await Bun.sleep(10)
  expect(reads).toBe(0)
})

const row = (seq: number, value = seq) => ({
  pipelineId: 'pipeline', seq, timestampMs: seq * 1000, row: { value },
})

test('a delayed pipeline snapshot preserves results delivered live during the read', async () => {
  let resolveSnapshot!: (rows: ReturnType<typeof row>[]) => void
  const pending = new Promise<ReturnType<typeof row>[]>((resolve) => { resolveSnapshot = resolve })
  let visible = [row(1)]
  const refresh = pending.then((snapshot) => { visible = mergePipelineResults(visible, snapshot, 200) })
  visible = [row(3), row(2), ...visible]
  resolveSnapshot([row(1), row(2)])
  await refresh
  expect(visible.map((entry) => entry.seq)).toEqual([3, 2, 1])
})

test('pipeline recovery deduplicates overlap, keeps current server values, and caps newest rows', () => {
  const current = [row(5), row(4), row(3, -1)]
  const snapshot = [row(1), row(2), row(3)]
  const result = mergePipelineResults(current, snapshot, 3)
  expect(result.map((entry) => entry.seq)).toEqual([5, 4, 3])
  expect(result[2]!.row.value).toBe(3)
  expect(current[2]!.row.value).toBe(-1)
})
