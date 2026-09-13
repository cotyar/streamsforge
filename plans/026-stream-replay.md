# 026 — Stream replay: subscribe from a position in the past, on any persistence mode

Status: **PLANNED** (2026-09-14). Orleans first; Dapr gets a PARITY line per wave and its own port later.

## Why

Orleans streams carry a `StreamSequenceToken` and the memory adapter IS rewindable
(`MemoryAdapterFactory.IsRewindable` returns true — verified by IL, `ldc.i4.1; ret`, against
Microsoft.Orleans.Streaming 10.3.0; the comment in `PushStreamProvider.cs:22` claiming otherwise is
wrong). But nothing in StreamsForge can subscribe "a little in the past": every one of the ~20
subscribers discards the token (`(evt, _) => …`), the PubSubStore is in-memory, the replay window is
one `SimpleQueueCache` of 4096 events per queue × 8 hash-ring queues shared by EVERY stream in the silo
(seconds of history under the seeded generators, then `QueueCacheMissException`), and the push
transport (`Streams:Transport push`) is not rewindable at all. What the platform does have is plan
023's `SourceReplayBuffer` (a 10 000-row in-memory ring, connector sources only) behind the
`BeginAttachAsync`/`EndAttachAsync` gate — exactly-once late attach, but always "everything I
remember", never "from here". The user asked for: subscribe to a stream from an earlier position,
independent of the entity's persistence mode, Orleans first.

## Scope

- IN: a producer-owned, sequence-numbered replay log for all five publish sites (connector,
  generator, ingest, pipeline results, table deltas); `from` (seq or timestamp) on the attach
  protocol, on gRPC `StreamService`, on `StreamHub`, and on consumer START (a table/pipeline created
  with `from`); an opt-in disk-backed log with retention so the position survives a restart and is
  independent of `TablePersistenceMode`; the four client SDKs + console; docs.
- OUT: Orleans-native tokens / a persistent queue provider (Azure Queue, EventHub, Kafka) — wrong
  window, new infrastructure, and no answer for the push transport (see decision D1); rewinding a
  LIVE consumer in place (D5); Dapr port (PARITY lines only; the log itself lives in `shared/` so the
  port is wiring, not design).

## Decisions

| # | Decision | Instead of |
|---|---|---|
| D1 | Replay is a **producer-owned log**, not Orleans tokens. `SourceReplayBuffer` generalizes to `ReplayLog` in `shared/StreamsForge.AppCore/Streaming/` (pure, unit-tested): every entry gets a monotonic `Seq` (per stream, from 1 per activation until D6 makes it durable) and the entry's own `_ts`; `Snapshot(ReplayFrom?)` returns entries with `Seq >= from.Seq` or `_ts >= from.TimestampMs`, plus `FirstSeq`/`LastSeq`/`TotalSeen` so "you got the last N of M" stays an honest, loggable statement. Eviction by count (10 000, unchanged) AND by age (`Replay:RetainMs`, default 0 = count only). | `SubscribeAsync(observer, token)` on memory streams: a 32k-event silo-wide window, dead on push, dead on restart. |
| D2 | The **attach gate is the replay API**. `IConnectorGrain.BeginAttachAsync()` gains an optional `ReplayFrom? from` (additive overload; the parameterless one keeps meaning "everything retained"). The same `IReplayable` shape (`BeginAttachAsync(from)`/`EndAttachAsync()`) is implemented by `GeneratorGrain`, a new `IngestSourceGrain` (D3), `PipelineGrain` (results) and `TableGrain` (deltas). Consumers keep plan 023's protocol verbatim: Begin → subscribe → feed snapshot through the same handler → End in a `finally`. | A second, token-shaped subscribe path next to the gate. |
| D3 | Ingest publishes from `OrleansFacades.DrainAsync` (a facade, no grain) today. The log needs a turn-based owner, so ingest rows go through a per-source `IngestSourceGrain` whose only job is `PublishAsync(rows)` + the gate + the log — one extra grain hop per batch, which the drain pump already amortizes per batch. `SourceIngressBuffer`, overflow policy and `DownstreamDropped` accounting are untouched. | A lock-guarded log inside the facade, which cannot honour the hold. |
| D4 | Wire position is a **new additive field**, `position` (int64, producer seq) on `SourceEvent`, `ResultEnvelope`, `TableDeltaBatch` and on the SignalR payloads. The existing `seq` stays the per-subscription counter it has always been (`EntityStreamFacade.cs`'s block comment pins that; clients rely on it). Requests gain `from_seq` / `from_timestamp_ms` (oneof-free: both optional, seq wins when both set). | Redefining `seq`, which is a behaviour change on every SDK. |
| D5 | Replay targets a **fresh executor only**. The Engine drops rows older than `Watermark` (`AllowedLatenessMs = 1000`, `ExecutorImpl.cs:28`), so rows replayed into a live pipeline/table are late events by construction. The consumer-side API is therefore `start(from)` — `TableDefinition.ReplayFrom` / `PipelineDefinition.ReplayFrom` (additive, `[Id(37)]` / `[Id(19)]`, a `{seq?, timestampMs?}` record keyed by input name) applied at `StartAsync`, when the executor is new. Changing it restarts the entity like any other executor-affecting field. A gRPC/SignalR subscriber has no executor and may ask for any retained position at any time. | Mid-life rewind, which would need a watermark reset and would silently emit retractions for windows already closed. |
| D6 | **Disk-backed log, opt-in per entity**: `replay: { persist: true, retainMs, maxBytes }` on the definition (additive). Append-only NDJSON segments under `DataDir/replay/<ns>/<qualified key>/NNNNNN.ndjson` (`{seq, ts, payload}`), rotated at `Replay:SegmentBytes` (default 8 MB), retained by age/bytes, written write-behind on the owning grain's existing flush cadence (same single-flight rule as `TableGrain`'s `FireAndForget`), read on attach by scanning from the first segment whose last seq ≥ `from`. `Seq` becomes durable (last seq persisted with the segment index) so a position handed to a client survives a restart. Independent of `TablePersistenceMode`: a `MemoryOnly` table with `replay.persist` keeps its delta log on disk and nothing else. | A log-consistency provider / `JournaledGrain` (needs a provider the project does not register; `TableHistoryGrain`'s class doc already argued this once), or reusing `TableJournalState` (per-key coalesced, not a sequence). |
| D7 | Table deltas replayed from the middle need a base. `ITableGrain.AttachSnapshotAsync` gains `LastSeq` (additive `[Id(2)]` on `TableAttachSnapshot`) so a consumer can take (rows, epoch, seq) atomically and ask for deltas `> seq` — the plan 023 backfill protocol with a position. `from` on a table-delta stream WITHOUT a snapshot is allowed (CDC/audit consumers want exactly the change log) and documented as such. | Refusing mid-log table subscriptions. |
| D8 | **Restart resume uses the log when it exists** — the bonus that justifies D6: a table whose inputs have `replay.persist` restarts by replaying its inputs from the start of retention into a fresh executor instead of "rebuilding purely from live traffic" (the RESTART-RESUME LIMITATION in `TableGrain`'s class doc). `Rebuilding` stays true until the replay reaches each input's `LastSeq` at boot. Without persisted inputs behaviour is byte-identical to today. | Leaving the operator-state limitation in place next to a log that can lift it. |
| D9 | The push transport gets nothing new: replay lives above the transport in both modes, which is the whole point of D1. `PushStreamProvider.IsRewindable` stays false; its wrong sentence about memory streams is corrected. | Teaching `PushStreamBus` cursors. |

## Waves

### Wave 1 — sources: `ReplayLog`, `from`, gRPC + SignalR (2–3 agents, Sonnet; worktrees)

Owns: `shared/StreamsForge.AppCore/Streaming/ReplayLog.cs` (+ `ReplayFrom` record in Contracts),
`SourceReplayBuffer.cs` (becomes a thin alias or is deleted with its tests moved),
`GrainInterfaces.cs` (additive), `ConnectorGrain.cs`, `GeneratorGrain.cs`, new `IngestSourceGrain.cs`
+ `OrleansFacades.DrainAsync`, `OrleansEntityStreamFacade.cs`, `EntityStreamFacade.cs` (additive
overloads with `ReplayFrom?`), `StreamGrpcService.cs`, `streamsforge.proto` (additive fields only),
`StreamHub.SubscribeSource(name, fromSeq?, fromTimestampMs?)`, Dapr `EntityStreamFanout` (accepts
and ignores `from`, logs once — PARITY line).

Acceptance:
- `ReplayLogTests`: seq monotonic from 1; `Snapshot(from seq)`/`Snapshot(from ts)` exact boundaries;
  count eviction at 10 000 and age eviction with an injected clock; `FirstSeq`/`LastSeq`/`TotalSeen`
  correct after eviction; snapshot is a copy. Existing `SourceReplayBufferTests` stay green unmodified
  (hard rule 1) or are moved verbatim.
- `SourceLateConsumerClusterTests` unchanged and green; new `SourceReplayFromClusterTests`: file source
  emits 500 rows; `BeginAttachAsync(from seq 401)` returns exactly 100 rows with `FirstSeq == 401`;
  `from timestampMs` at the 300th row's `_ts` returns ≥ 200 rows and none older; a generator and an
  ingest source pass the same shape (the ingest one via `POST /api/sources/{name}/events`, proving the
  D3 hop loses nothing: 1 000 pushed, 1 000 in the table, `DownstreamDropped == 0`).
- gRPC: `SubscribeSource{name, from_seq: 401}` yields events with `position` 401…500 then live rows;
  `position` is producer-assigned (two concurrent subscribers see the same `position` for the same row,
  different `seq`). SignalR: the same via `SubscribeSource(name, 401, null)`.
- Whole Orleans suite green except the 6 known failures (plans/022); Dapr solution builds and its
  suite is green.

### Wave 2 — pipeline results and table deltas as replayable streams (2 agents; Opus for TableGrain)

Owns: `PipelineGrain.cs` (results log + gate), `TableGrain.cs` (delta log + gate + `LastSeq` on
`AttachSnapshotAsync`), consumers `TableGrain.AttachToTableInputAsync` / `AttachToPipelineInputAsync`,
`TableIngestGrain.cs`, `ArrangementGrain.cs`; `ReplayFrom` on `TableDefinition`/`PipelineDefinition`
(D5) applied in `StartAsync`; `StreamGrpcService` `SubscribePipeline`/`SubscribeTable` `from_*`;
`StreamHub.SubscribeTable(idOrName, fromSeq?, fromTimestampMs?)` / `SubscribePipeline`.

Acceptance:
- `TableAttachSnapshot.LastSeq` equals the `position` of the last delta batch a concurrent subscriber
  received before the snapshot, never after (the existing `BackfillOnAttachClusterTests` shape, plus
  seq).
- A table created with `replayFrom: { "<source>": { seq: 1 } }` over a source that already emitted 500
  rows before the table existed AND before the ring would have been handed over anyway yields the same
  500 (regression guard: `from` never returns less than plan 023's default attach).
- A table over a pipeline created after the pipeline emitted 300 results, with `replayFrom` at result
  seq 101, holds exactly the rows results 101…300 produce (windowed pipeline SQL, so this also proves
  D5: the fresh executor's watermark starts at the first replayed `_ts`, no `LateEvents`).
- `SubscribeTable{name, from_seq}` on gRPC streams the retained delta batches from that position;
  with `from_seq` older than retention the first batch carries `position == FirstSeq` and the RPC
  trailer/first message carries a `replay_truncated: true` flag (additive) — never a silent gap.
- Changing `ReplayFrom` on a Running table restarts it (`Revision` bumps, status cycles), same as an
  SQL edit.

### Wave 3 — persisted log, retention, restart resume (Opus, may spawn 2 subagents)

Owns: `shared/StreamsForge.AppCore/Streaming/{ReplaySegmentWriter,ReplaySegmentReader,ReplayRetention}.cs`
(pure; NDJSON segments, rotation, retention by age/bytes, crash-safe: a torn last line is dropped on
read, never fails the open), `ReplayOptions` (`Replay:RetainMs`, `Replay:SegmentBytes`,
`Replay:MaxBytesPerStream`), `replay` config group on all three definitions (additive Ids:
Source `[Id(17)]`, Pipeline `[Id(20)]`, Table `[Id(38)]` — wave 2 takes 19/37), the five owning grains'
flush wiring, `RegistryGrain.EnsureInitializedAsync` / `BootResume` for D8, `GET /api/{kind}/{id}/replay`
(`{firstSeq, lastSeq, retainedBytes, persisted}`), `ImportPlanner`/`CatalogRecordMerge` (the new group
round-trips through config export/import), `SecretWalk` untouched (no secrets in the group).

Acceptance:
- `ReplaySegmentTests`: write 25 000 entries at 8 MB segments → ≥ 2 segments; read from seq 12 345
  returns exactly 12 656 entries in order; a segment truncated mid-line reads to the last full line;
  age retention deletes whole segments only; `MaxBytesPerStream` keeps the newest segments.
- `StreamsForge.Chain.Tests.ReplayRestartTests` (spawned host, ports 6899/6999 http/grpc, silo
  16899/36899 — added to CLAUDE.md's port sentence): a `url` source with `replay.persist` and a dedup
  key emits 200 rows; `Kill(entireProcessTree)`; restart with the same `DataDir`;
  `SubscribeSource{from_seq: 1}` over gRPC yields 200 rows with positions 1…200 BEFORE the source's
  first post-restart poll; a `MemoryOnly` table over that source comes back with 200 rows and
  `rebuilding == false` within 30 s (D8) — the pre-plan behaviour is an empty table, and the test
  asserts the difference, not just the end state.
- A `Batched` table with NO `replay.persist` on its inputs restarts exactly as before (the existing
  `HostRestartTests` stay green unmodified).
- Disk bound proven: 1 M rows of the seeded generator with `MaxBytesPerStream = 64 MB` never exceeds
  64 MB + one segment on disk (`du` in the test).
- Live check on an isolated host (`--Http:Port 6899 …`): enable `replay.persist` on `trades`, restart,
  `grpcurl … from_seq` replays, `/api/sources/trades/replay` reports `persisted: true`.

### Wave 4 — SDKs, console, docs (Sonnet, 3 agents: TS+web / .NET+Python+Kotlin / docs)

- TypeScript (`clients/typescript`: `subscribeTable(name, { from: { seq } | { timestampMs } })` on both
  transports, `position` on every event), .NET, Python, Kotlin: the same option; one live test per SDK
  (`from_seq` returns the expected count) on the existing per-SDK fixture ports.
- Console: a "Replay from…" control on the source/table detail live tape (seq or timestamp) and the
  `replay` group in the definition editors; `web/src/api/types.ts` additive.
- `admin/sf.ts rows --from-seq`, MCP tool option.
- Docs: `orleans/docs/index.html` § Stream replay (what is retained, in-memory vs persisted, the D5
  rule, the truncation flag), `orleans/ARCHITECTURE.md`, `TRANSPORTS.md` (a transport author gets the
  log for free through `PublishAsync`), `CLAUDE.md` paragraph + ports, `dapr/PARITY.md` (one line per
  wave), `PushStreamProvider` comment fix (D9).

## Gates (every wave)

```bash
~/.dotnet/dotnet build orleans/StreamsForge.sln
~/.dotnet/dotnet test  orleans/StreamsForge.sln --filter "FullyQualifiedName~Replay|FullyQualifiedName~SourceLateConsumerClusterTests|FullyQualifiedName~BackfillOnAttachClusterTests|FullyQualifiedName~ConnectorGrainClusterTests|FullyQualifiedName~TableJournalClusterTests|FullyQualifiedName~HostRestartTests"
~/.dotnet/dotnet test  orleans/StreamsForge.sln          # full; green except the 6 known pre-existing failures (plans/022)
~/.dotnet/dotnet build dapr/StreamsForge.Dapr.sln && ~/.dotnet/dotnet test dapr/StreamsForge.Dapr.sln
```

Never through `tail`; grep `^(Passed!|Failed!)`. A time-bounded failure is re-run under `--filter`
and reported both ways. Commit and push after each wave.

## Known limits, stated up front

- In-memory positions (waves 1–2) restart from 1 on activation; a client holding a position across a
  restart gets `replay_truncated` unless the entity has `replay.persist` (wave 3).
- Sharded tables (plan 011 D2) publish deltas per shard through the router; the table-level log is
  fed by the router's merged stream, so a sharded table's position is the router's, not a shard's.
- `crdt` sources keep their own replay (`ReplayAsync`, plan 020) and are not logged; a `from` on them
  is refused with a named reason.
- Dapr: `EntityStreamFanout` accepts and ignores `from` until the port; the envelopes already carry
  `Seq` (`Streaming/Envelopes.cs:58`), so wave 1's PARITY debt is the gate + `ReplayLog` wiring only.
