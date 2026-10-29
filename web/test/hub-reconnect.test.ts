import { afterEach, expect, mock, test } from 'bun:test'

let reconnect: (() => Promise<void>) | undefined
let invokeImpl: (method: string) => Promise<void> = async () => {}
const invokes: string[] = []
const connection = {
  on() {},
  onreconnected(handler: () => Promise<void>) { reconnect = handler },
  start: async () => {},
  stop: async () => {},
  invoke(method: string) { invokes.push(method); return invokeImpl(method) },
}
class HubConnectionBuilder {
  withUrl() { return this }
  withAutomaticReconnect() { return this }
  build() { return connection }
}
mock.module('@microsoft/signalr', () => ({ HubConnectionBuilder, HttpTransportType: {} }))
const { subscribeTable, subscribePipeline, onHubReconnected, disconnectHub } = await import('../src/realtime/hub')

afterEach(async () => {
  invokeImpl = async () => {}
  await disconnectHub()
  invokes.length = 0
})

test('reconnect signals views without waiting for unrelated group acknowledgements', async () => {
  const unsub = subscribeTable('orders', () => {})
  const pipeline = subscribePipeline('pipeline-id', () => {})
  await unsub.ready
  let refreshes = 0
  const stopRefresh = onHubReconnected(() => { refreshes++ })
  let release!: () => void
  // Use one shared promise for both group registrations.
  const registered = new Promise<void>((resolve) => { release = resolve })
  invokeImpl = () => registered
  const restoring = reconnect!()
  expect(refreshes).toBe(1)
  expect(invokes).toContain('subscribePipeline')
  expect(invokes).toContain('SubscribeTable')
  let pipelineReady = false
  void pipeline.ready.then(() => { pipelineReady = true })
  let ready = false
  void unsub.ready.then(() => { ready = true })
  await Promise.resolve()
  expect(ready).toBe(false)
  expect(pipelineReady).toBe(false)
  release()
  await restoring
  expect(ready).toBe(true)
  expect(pipelineReady).toBe(true)
  expect(refreshes).toBe(1)
  stopRefresh()
  await reconnect!()
  expect(refreshes).toBe(1)
})

test('a failed group registration still lets views recover their REST snapshot', async () => {
  const unsub = subscribeTable('orders', () => {})
  await unsub.ready
  let refreshes = 0
  onHubReconnected(() => { refreshes++ })
  invokeImpl = async () => { throw new Error('connection interrupted') }
  await reconnect!()
  expect(refreshes).toBe(1)
  await expect(unsub.ready).rejects.toThrow('connection interrupted')
})

test('pipeline snapshot registration is acknowledged before reading, then rearmed on reconnect', async () => {
  let release!: () => void
  const registered = new Promise<void>((resolve) => { release = resolve })
  invokeImpl = () => registered
  const pipeline = subscribePipeline('pipeline-id', () => {})
  let acknowledged = false
  void pipeline.ready.then(() => { acknowledged = true })
  await Promise.resolve()
  await Promise.resolve()
  expect(acknowledged).toBe(false)
  release()
  await pipeline.ready
  expect(acknowledged).toBe(true)
})
