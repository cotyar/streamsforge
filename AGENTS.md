# crates-foundation — Agent Instructions

Streaming-SQL platform ("StreamsForge") in two runtime flavors: `orleans/` (**primary**, complete —
Microsoft Orleans 10) and `dapr/` (Dapr, for polyglot processing and runtime comparison — feature-complete
in code, and as of plan 025 mostly closed on live verification too: gRPC, TLS, boot order, late-consumer
replay and table-over-pipeline are all now proven live on an isolated Dapr instance. What is still
unverified there (database connectors/CDC, FIX/fix-duplex, NATS, peer-name federation FROM a Dapr
instance) is smaller than it used to be but not zero: read [`dapr/PARITY.md`](dapr/PARITY.md) before
claiming anything about those, and land new work on Orleans first). Both
flavors share one runtime-agnostic core (`shared/`): Engine, Contracts, AppCore, Api, and the `web/`
SPA. Execution plans with acceptance criteria: [`plans/`](plans/README.md). Adding an ingress/egress transport
(NATS + a `file` sink today; the recipe is one class + one registry line): [`TRANSPORTS.md`](TRANSPORTS.md) —
which also covers **console UI plugins**: an out-of-tree library can replace the generic config form for its
own source/sink kind with its own React editor by dropping one ES module in the host's `ui-plugins/`
directory (`GET /api/ui-plugins`, `Ui:PluginsPath`), with no change under `web/` — the plugin gets the
console's own React, authenticated `api`, its single SignalR connection (`live.subscribe*`) and a lazy
`loadLiveTables()` (TanStack DB) off `window.streamsforge`, `apiVersion: 2`. The SERVER half is the same
shape: an `IStreamsForgePlugin` in the host's `plugins/` directory (`Plugins:Path`) registers its own
transports at startup, and its config lives in the open **`settings` bag**
(`ConnectorConfig.Settings`/`SinkSpec.Settings`, a string dictionary — masked by the kind's own
descriptor, since `SecretWalk` cannot see into a dictionary; a kind whose plugin is absent masks its
WHOLE bag, failing closed). So an out-of-tree kind adds a config dimension without touching
`StreamsForge.Contracts`; the one thing the bag cannot express is a nested optional group. Operator-facing
install instructions: `orleans/docs/index.html` §§ Server plugins & out-of-tree kinds / Console UI
plugins; contributor-facing guide (both hooks, worked in-tree examples, the ILRepack build rule):
[`PLUGINS.md`](PLUGINS.md). Architecture:
[`orleans/ARCHITECTURE.md`](orleans/ARCHITECTURE.md) · [`dapr/ARCHITECTURE.md`](dapr/ARCHITECTURE.md)
· Dapr's descoped/owed/unverified list: [`dapr/PARITY.md`](dapr/PARITY.md)
· rationale: [`orleans/DESIGN.md`](orleans/DESIGN.md) · runtime comparison + measured latency:
[`orleans/docs/comparison.html`](orleans/docs/comparison.html) (opened directly from the repo — see
its own note on why `/docs` doesn't serve it automatically).


**CSV I/O** (plan 012): `format: "csv"` on a url/file/folder/nats source sniffs its delimiter from the
header line, so TSV / semicolon / pipe exports read through the same format; the `file` sink kind
appends CSV or NDJSON to a path on the host (append-only, header fixed for the life of the file); and
`GET /api/tables/{id}/rows.csv` + `GET /api/pipelines/{id}/results.csv` (plus a **CSV** button on both
detail pages) download the same rows. One writer, `CsvFormatter`, behind all of it.

**Native CDC** (plan 017): `postgres-cdc` and `mssql-cdc` are two more `IPolledTransport` source kinds,
living in `shared/StreamsForge.Connectors.Database` alongside the plain `postgres`/`mssql` kinds — a
source reads the database's own change log (Postgres logical replication, SQL Server capture tables)
instead of polling a cursor column. Their operational hazards (an undrained Postgres slot pinning WAL,
SQL Server's 3-day CDC retention default, `REPLICA IDENTITY FULL`) are written down in
[`TRANSPORTS.md`](TRANSPORTS.md)'s "Change data capture" section — read it before enabling either kind.

**FIX** (plan 018): `format: "fix"` is a fifth payload format — tag=value, delimiter sniffed, no FIX
dictionary (a static table of the common 4.2/4.4/5.0 tags, unknown tags fall back to `tag<N>` strings),
repeating groups parsed into nested JSON arrays — usable by any `url`/`file`/`folder`/`nats` source; and
`fix` is also a live, receive-only session source kind, `shared/StreamsForge.Connectors.Fix` on
`QuickFIXn.Core`, an `IInboundTransport` out of the core like the database connectors. **As of plan 022
the `fix`/`fix-duplex` session kinds ship as the `StreamsForge.Plugins.Fix` server plugin under
`plugins/`** (merged with `QuickFix`), not a host reference — the wire `format: "fix"` itself stays in
`StreamsForge.AppCore` and needs no plugin, only the live session kinds do; see the "Plugins &
single-file" paragraph below. No FIX dictionary
ships with the platform (`UseDataDictionary=N`); order entry is deliberately a separate plan
([`019`](plans/019-fix-order-entry.md)). The session's operational hazards (the drop-oldest bridge
queue, `storePath`'s in-memory-vs-file-backed choice, at-most-once delivery) are written down in
[`TRANSPORTS.md`](TRANSPORTS.md)'s "FIX" section — read it before enabling the kind.

**FIX order entry** (plan 019, DONE): `fix-duplex` is a third transport seam, `IDuplexTransport` —
one live FIX session with both halves, declared as two entities that meet in the middle (a `fix-duplex`
source owning the session, a `duplex` sink on any pipeline/table naming it by `sourceName`). The session
lives in the connector driver (Orleans `ConnectorGrain` / Dapr `ConnectorActor`), not the sink, which is
why an unrelated field edit on the sink — which does tear down and rebuild that sink's client every 30s
— costs nothing: the sink owns no connection, only a `DuplexSessions.Find` lookup by name. A row's
`MsgType` column picks the outbound message; `FixRequiredFields` gates `NewOrderSingle`/
`OrderCancelRequest`/`OrderCancelReplaceRequest` against a curated table (no real dictionary); `ClOrdID`
generation is opt-in and unchecked for uniqueness when caller-supplied. `storePath` stops being optional
on this kind — an order session's sequence store is the record of what was sent, not a resend-avoidance
nicety. Execution reports correlate to the orders that caused them through ordinary platform SQL, joined
on `ClOrdID` — plan 019 D7's cheapest large win. Full wiring, the row→FIX mapping rules, and a gotcha
found live (every row a duplex sink actually forwards carries platform-reserved columns — `_ts`/
`_source`/`_weight` — that the outbound mapper must skip rather than refuse) are in
[`TRANSPORTS.md`](TRANSPORTS.md)'s `fix-duplex` sections.

**Plugins & single-file** (plan 022): three previously-linked features — the Quant pricing scalars,
the `fix`/`fix-duplex` transports, and (Orleans only) `crdt`'s grain-backed facade — now ship as
install-time server plugins under `plugins/` (`StreamsForge.Plugins.{Quant,Fix,Crdt}`), each merged by
ILRepack into one self-contained DLL an operator installs by copying it into `plugins/`. **Ownership
rule: neither host csproj links a plugin project as a normal reference** — every plugin
`ProjectReference` carries `ReferenceOutputAssembly="false"` (build order only), and two host-owned
MSBuild targets (`CopyBuiltInPlugins`/`PublishBuiltInPlugins`) copy each plugin's merged DLL into
`$(OutDir)plugins`/`$(PublishDir)plugins` so `dotnet run` and `dotnet publish` both have them with no
manual step. `IStreamsForgePlugin.Register()` covers a plugin that only adds transports or SQL
functions (Quant, Fix); `IStreamsForgeWebPlugin` (`StreamsForge.Api`, not `AppCore` — hard rule 2) adds
`ConfigureServices`/`MapEndpoints` for a plugin that needs the host itself, which is what Crdt uses to
replace the core's disabled `ICrdtFacade` stub with the real Orleans one. **Publish knobs live only in
each host's `Publish.props`** (imported by the csproj when present, everything gated on MSBuild's own
`_IsPublishing` so `dotnet build`/`dotnet run` stay byte-identical): `PublishSingleFile` +
`SelfContained`, explicitly **no trimming and no Native AOT** (dynamic protobuf, gRPC reflection,
SignalR and the plugin loader's `Activator`/`AssemblyLoadContext` use are all reflection paths a
trimmer breaks silently), plus the SPA/docs/protos embedded-with-disk-first-fallback wiring
(`EmbeddedPublishContent` in `shared/StreamsForge.Api/StreamsForgeApiExtensions.cs`). `tools/publish.sh
<orleans|dapr> [rid] [out-dir]` drives it end to end; both Dockerfiles under `deploy/` now run the
published native executable directly rather than `dotnet <name>.dll`. Full guide (the hook decision
table, all three in-tree plugins as worked examples, the ILRepack merge rule, and the runtime failure
modes when a plugin is absent): [`PLUGINS.md`](PLUGINS.md). Console UI plugins are now `apiVersion: 3`
(2→3 added `draft`/`onSuggest`, name/description suggestions applied only while the user's own field is
still blank) and can be a single TypeScript file (`.ts`/`.tsx`, no `import`s) transpiled in the browser
via a lazily-downloaded `sucrase`; `GET /api/ui-plugins` is `Cache-Control: no-store` with a `?v=` on
every URL, so a plain reload (not a hard one) picks up an edited file. The server-side loader is now
two-pass (load every DLL in `plugins/` before scanning any for `IStreamsForgePlugin` — order-independent)
and logs a line when a plugin references a newer assembly version than the host has loaded. **Six
pre-existing test failures, not
caused by this plan**: restart/reactivation tests in `CrdtDocGrainClusterTests` (3),
`ShardedTableClusterTests` (2) and `ConnectorGrainPolledClusterTests` (1,
`ADeactivatedConnectorResumesFromThePersistedCursor`) fail deterministically with
`CodecNotFoundException(Newtonsoft.Json.JsonSerializationException)`, verified failing before this
work at `bfc421f`; see `plans/022-plugins-and-single-file.md`'s found-and-not-fixed list for status.
Test counts move slightly from this plan's own AppCore.Tests additions (the plugin-loader and
`UiPluginsEndpointsTests` coverage) — treat the exact whole-suite numbers elsewhere in this file as
approximate until the next verification pass confirms them.

**Entitlements, approvals, audit** (plan 015): authorization is per-resource grants, not the three
role strings. A grant is `action` (`pipeline.update`, `*`) × `scope` (`*` | exact entity **NAME** |
prefix `prod-*` | `tag:finance`) × `Allow`/`Deny`, deny-overrides, attached to a user, a group or a
role; `AccessGuard` asks it in every handler while the coarse `Viewer/Editor/Admin` policies stay on
every route. **Permissions resolve server-side per request, never from the JWT** — a revocation lands
within `Auth:PolicyCacheSeconds` (default 10). `Auth:Mode=legacy` is the one-flag rollback and the
default is `entitlements`, so a typo in that value leaves enforcement **on**. `access.write` (not
`user.write`) satisfies the coarse `Admin` policy — whatever satisfies it is the key to `/api/access`
itself. Scope is the NAME because ids are `Guid("n")` and match no `prod-*` grant. Optional approvals
(`Approvals:Enabled`, off by default; you cannot vote on your own request; approving records, it does
not execute) and a day-sharded audit log whose `truncated` counter exists so silence is never read as
absence. Masked `beforeJson`/`afterJson` are opt-in (`?includeChanges=true` + `access.read`) — a
credential rotation reads `"***" → "***"`, the key's presence is the signal. Routes:
`shared/StreamsForge.Api/Endpoints/{AccessEndpoints,ApprovalsEndpoints,AuditEndpoints}.cs`; the pure
decision: `shared/StreamsForge.AppCore/Access/PermissionEvaluator.cs`. Operator recipes and the full
gotcha list: the `sf-access` skill and `orleans/docs/index.html` § Roles, entitlements & approvals.

**Name resolution, versioning & discovery** (plan 016): every `GET /api/{sources|pipelines|tables}/{key}`
resolves the segment as an ordinal **id, else name** — 1 match, 0 → 404, ≥2 → **409** naming every candidate
id, never a silent first match. **`PUT`/`DELETE`/`start`/`stop` stay id-only** — a name a `GET` resolves
happily still 404s on a write to the identical URL. Sources have no id and can never be renamed; a table
renames only while `Stopped`, unsharded and unreferenced; pipeline names are enforced unique going forward.
Two registry-assigned counters — `Revision` (any change) and `SchemaRevision` (sources/tables, shape only)
— back `dependsOn: [{kind, name, schemaRevision}]` pins on pipeline/table config documents: a violated pin
sets `staleReason` and rolls into `catalogWarnings` as a count, and it never stops the entity.
`mode=validate` reports it as a `dependsOn:` diagnostic before anything is applied — **except** for a pin
naming an entity the same document declares, whose post-import revision is registry-assigned and so
unknowable at plan time; that one only surfaces as `staleReason` after the write. Config import is schema-gated by
default (`schemaPolicy: "any"` is the one string that turns it off) and can declare
`requires: [{kind, version}]` connector versions that refuse the **whole** import when unsatisfied — like
every per-entity refusal here, that comes back as **HTTP 200** with `ok:false`, so `curl -f` reads a
refusal as success. `GET /api/meta/instance` (anonymous) plus a configured `Discovery:Peers` directory let
a `grpc` source federate by **peer name + entity name** — no address, no GUID (`GrpcSubConfig.Peer`,
resolved fresh at each reconnect, wins over a literal `Address`/`RestAddress` when set); the peer directory
has no heartbeat or expiry, so a peer that went away is not noticed until re-probed. `@name` resolves any
endpoint-shaped config field (host/URL/connection string) from `Endpoints:<name>` at connect time, never
from the catalog and never written back on export — values read back unmasked at `GET /api/meta/endpoints`.
Full routes and verified live recipes: `orleans/docs/index.html` §§ REST API / Instance discovery &
federation / Configuration import-export; contributor-facing `@name` wiring: `TRANSPORTS.md`'s Named
external endpoints section.

**TLS** (plan 024 on Orleans, plan 025 on Dapr — both flavors now): `Tls:Enabled=true`
plus the standard `Kestrel:Certificates:Default` section (`Path`+`KeyPath` PEM, `Path`+`Password`
PFX, or `Subject`+`Store`) puts HTTPS on the REST port and TLS (ALPN h2) on the gRPC port; startup
fails fast when the flag is set with no certificate. It only applies to the two-listener branch of
`Program.cs` — a `--urls` deploy (Docker, Cloud Run) turns TLS on through `--urls https://…` plus the
same certificate section, and with real TLS that single port ALSO serves gRPC, which a cleartext
single port never could. `Tls:Enabled` also enables HSTS (not emitted for loopback hosts — ASP.NET's
`ExcludedHosts` default, deliberately kept). Behind a TLS-terminating proxy set the built-in
`ASPNETCORE_FORWARDEDHEADERS_ENABLED=true` (trusts any proxy — off by default, fail-closed) so
`/api/meta/instance` reports `https://` endpoints to peers. Every outbound HTTPS/gRPC connection the
host makes (`url` source, `http` sink, `grpc` source, peer probes, OpenAPI schema derivation) goes
through `shared/StreamsForge.AppCore/Net/OutboundTls.cs`: `Tls:TrustedCaPath` (PEM bundle) EXTENDS
system trust — a name mismatch still fails — and `Tls:AcceptAnyCertificate` (dev only, warned at
startup) disables it; configure before the first outbound call, the class latches. `tools/tls/dev-cert.sh
<out-dir>` mints a self-signed cert that is its own trust anchor. NATS kinds carry a `tls` config
group (`caFile`/`certFile`/`keyFile`/`insecureSkipVerify`). All four client SDKs accept a
scheme-carrying gRPC target (`https://host:port`; scheme-less stays plaintext), a CA-file option and a
dev-only skip-verify (TypeScript `ca`/`verify:false`/`STREAMSFORGE_CA`; .NET `CaCertificatePath`/
`AcceptAnyCertificate`; Python `ca=`/`verify=False` REST-only; Kotlin `caFile`/`insecure`); the admin
CLI/MCP read `SF_CA_FILE`/`SF_INSECURE=1`. Proven end to end by `TlsHostTests`, `TlsChainTests` (two
hosts, folder → gRPC over TLS → table, 200/200), `ForwardedHeadersTests` and one live TLS suite per
SDK. Also found and fixed here: since plan 022 a bare `dotnet publish` (no `-r`) defaulted to
**linux-x64** single-file, so every client fixture on a Mac published an unrunnable host —
`Publish.props` now defaults to the SDK's own RID (`$(NETCoreSdkRuntimeIdentifier)`; `tools/publish.sh`
and the Dockerfiles pass `-r` explicitly and are unchanged), and the fixtures run the native
executable when there is no `.dll`. Operator recipes: `orleans/docs/index.html` § TLS; the survey that
sized this: `plans/024-tls-support.md`.

**Dapr TLS (plan 025) mirrors the above exactly** — same flag, same `Kestrel:Certificates:Default`
section, same fail-fast, same HSTS, same `OutboundTls`. The one Dapr-only fact: the sidecar itself
calls the app port for every actor invocation and topic delivery, so a TLS app port also needs
`dapr run … --app-protocol https` (`dapr/tools/run.sh`'s `DAPR_RUN_EXTRA_ARGS` env var carries it) —
daprd does not verify the app's certificate on that channel, so a self-signed dev pair needs nothing
further. Verified live: https healthz 200, plain http gets an empty reply, `https://` endpoints in
`/api/meta/instance`, `grpcurl -cacert` streaming over the TLS gRPC port, seeded tables still filling
over https. See `dapr/ARCHITECTURE.md`'s TLS section.

**Dapr parity (plan 025)**: closes most of `dapr/PARITY.md`'s debt list plus the TLS gap plan 024 left.
The seven gRPC services (Source/Pipeline/Table/Stream/Ingest/DynamicStream/ServerReflection) moved out
of the Orleans host into `shared/StreamsForge.Api/Grpc/`, retargeted onto the environment-scoped
`ICatalogFacadeFactory` both hosts already had plus one new runtime primitive,
`IEntityStreamFacade` (`shared/StreamsForge.Contracts/EntityStreamFacade.cs` — subscribe one entity's
live stream for the life of one RPC call; Orleans implements it over its stream provider, Dapr over an
in-process per-key fan-out fed by the fixed pub/sub topics, `Streaming/EntityStreamFanout.cs`) — so the
Dapr host now serves gRPC on `:5499` too, `/api/meta/instance` reports it, and it can be a federation
server. Late-consumer replay (`IConnectorActor.BeginAttachAsync`/`EndAttachAsync` + the shared
`SourceReplayBuffer` ring), coordinated boot order (`Services/BootResume.cs`'s `BootGate`, one ordered
pass per environment: pipelines → tables topo-sorted → sources), the paced source relay (ported from
Orleans' 50ms-slot pacer, replacing a drop rule), table-over-pipeline (`PipelineDefinition.OutputFields`
offered as a table relation, the three name-collision refusals), and an honest `crdt` Failed status
(no actor activated) all port from Orleans to this flavor. **Two Dapr-only facts found by the live
tests, both about the data path racing the pipeline watermark**: the SignalR bridge must never sit ON
the data path (the first pacing port awaited its 50 ms slot inside the `sf-sources` endpoint, whose
sinks run sequentially with the bridge first — a 50-row batch reached the pipeline 2.5 s late and
was dropped whole; `DaprStreamBridge` now enqueues and paces on its own task), and the Engine's 1 s
lateness allowance is sized for Orleans' in-process hop, not Dapr's sidecar hops under CPU pressure —
`PipelineActor`'s wall-clock watermark tick runs `Pipelines:TransportLagAllowanceMs` (default 4000)
behind, so windows close 4 s later on Dapr and the loss is visible either way as
`PipelineMetrics.LateEvents` (`lateEvents` on `GET /api/pipelines/{id}/metrics`, both flavors — a
growing count with `totalRowsOut` flat is the signature). A new isolated Dapr test instance finally
makes live verification possible — see the port table above and `dapr/PARITY.md` §3 for why this had
never been done before plan 025 (a 164-commit boot-breaking regression, then no isolated-instance mode).
Full decision list and what remains unverified (database connectors/CDC, FIX/fix-duplex, NATS on Dapr,
peer-name discovery FROM a Dapr instance): `dapr/PARITY.md`; the wiring: `dapr/ARCHITECTURE.md`.

**Stream replay** (plan 026, wave 1 on Orleans): every source driver (connector, generator, ingest —
the new `IngestSourceGrain` is the turn-based owner ingest publishing never had) keeps a
producer-owned, sequence-numbered `ReplayLog` behind plan 023's attach gate (`SourceReplayGate`, one
class shared by all three), and `IReplayableSourceGrain.BeginAttachAsync(ReplayFrom?)` hands a
consumer the retained rows from a **position or an event time** with `Positions/FirstSeq/LastSeq/
Truncated`. gRPC `SubscribeSource{from_seq|from_timestamp_ms}` and the hub's `SubscribeSourceFrom`
use it through `IEntityStreamFacade`'s replaying overload; `SourceEvent.position` is now written on
every event (the same number for every subscriber; `seq` stays per-subscription) and
`replay_truncated` on the first one when the request reached past retention. Rules that bite:
Orleans' memory adapter IS rewindable (verified by IL) but only within a 4096-event cache per
hash-ring queue shared silo-wide, which is why replay lives above the transport; `init` setters on a
`[GenerateSerializer]` record fail codegen (`ORLEANS0101`); live positions are COUNTED from the
attach snapshot's `LastSeq`, so a subscription landing inside plan 023's one-pull-period gap can run
one ahead (tests quiesce 2 s first); positions restart at 1 per activation until wave 3's
persisted log; Dapr accepts and ignores `from` (`dapr/PARITY.md` § 2b D11). **Wave 2** extended the
same gate (`ReplayGate<T>`) to pipeline result batches and classic-mode table delta batches (a
position is a published BATCH; coordinator-mode tables publish from `TableOutputGrain` and replay
nothing yet), gave gRPC `SubscribePipeline`/`SubscribeTable` and the hub's `SubscribePipelineFrom`/
`SubscribeTableFrom` the same `from` + `position`, and added **`replayFrom`** (`{ "<input>": { seq |
timestampMs } }`) to table and pipeline definitions: applied at START only into a fresh executor
(replaying into a live one makes every row a late event), so changing it on a Running entity
restarts it; a named source input (connector, generator or ingest) or pipeline input attaches from
that position, an unnamed one keeps the pre-026 path; a table input, an unknown input or a crdt
source is refused at create/update. A `replayFrom` edit does not bump `Revision` (the config
projection does not carry it yet — wave 3). Dapr stores `replayFrom` but `TableActor`/`PipelineActor`
refuse to start with it set, like `shardBy`. Plan, decisions D1–D9 and per-wave outcomes:
`plans/026-stream-replay.md`.

**Environment isolation** (plan 021): an environment is the **registry KEY**, not a column — Orleans
activates a whole separate `RegistryGrain` per environment (Dapr: `RegistryActor`), so two same-named
tables in two environments are two grains with two states, not one filtered read. `default` is the
**empty string** internally and renders as `default` only in the API and console; `EnvKeys.Qualify("", k)
== k`, which is why an existing `data/` directory and an existing Redis store come up byte-identical —
there is no migration. The separator is `.`, **not** the `:` the plan's own D3 named — overruled in wave
0, because `JsonFileGrainStorage` sanitizes every state-filename character outside `[A-Za-z0-9_.-]` to
`_`, and `_` is legal in an entity name: `staging:orders` and a default-environment table named
`staging_orders` would sanitize to the SAME file. A source/table name may not contain `.` going forward
(refused at create/rename), which costs nothing real — the SQL tokenizer already can't reference a dotted
identifier. The ambient (`EnvironmentAmbient`, an `AsyncLocal`) is **request-only**, set by exactly one
middleware and read only where a runtime key is composed; background work — supervisors, the lifecycle
orchestrator, connector drivers — never reads it and instead reads `Environment` off the definition it is
acting on, because the ambient is empty outside a request and empty silently means default. **Seeding is
default-environment-only**: `RegistryGrain.EnsureInitializedAsync` runs on every environment's registry at
boot (so already-Running entities resume everywhere), but its three seed blocks are gated on
`_env == EnvKeys.Default` — otherwise creating an empty `staging` and restarting would fill it with the
demo catalog, and force-deleting an environment's contents would silently re-seed them on the next boot.
Environments are a **namespace, not a security boundary** until 015's grants are scoped to one (the
plan's D9) — any authenticated Editor can point `X-StreamsForge-Environment` at any environment today.
Full routes, the console picker, force-delete semantics and the gotcha list: `orleans/docs/index.html`
§ Environments; contributor-facing key composition: `orleans/ARCHITECTURE.md` / `dapr/ARCHITECTURE.md`;
the `/sf-env` skill; per-wave outcomes and the found-and-not-fixed list at the end of
`plans/021-environment-isolation.md`.

## Environment — non-negotiables

- **dotnet**: `~/.dotnet/dotnet` (SDK 10.0.3xx). It is **NOT on PATH** — always use the full path.
- **JS tooling**: **bun ≥ 1.4, never npm.** The repo root is a **bun workspace** (`package.json`,
  members `web` + `clients/{typescript,tanstack-db,react}`) with ONE `bun.lock`: the local
  `@streamsforge/*` packages are LINKED, so an edit in `clients/typescript` is what `web` compiles
  against. Before that they were `file:` deps, which bun COPIES — and since `dist/` is gitignored, a
  fresh clone copied a package with no build output and `bun run build` failed on
  `Cannot find module '@streamsforge/client'`. `web`'s `prebuild` compiles both client packages, so
  `bun run build` in `web/` (or `bun run --cwd web build` from the root) is one command from a clean
  checkout. Bun 1.4 links workspaces **isolated** (pnpm-style), so a package must DECLARE every module
  it imports or augments — an undeclared transitive dependency that used to be hoisted into reach now
  fails to resolve.
- **Ports**: Orleans dev server owns `5199` (REST/SignalR/SPA) + `5299` (gRPC h2c) and is often
  running — never bind or kill it. Dapr flavor: app `5399` (REST/SignalR/SPA), gRPC `5499` (plan 025:
  all six services + reflection served here now, not just reserved), sidecar HTTP `3599` / gRPC `4599`;
  run via `dapr/tools/run.sh` (dapr runtime 1.18.x, containers `dapr_redis`/`dapr_placement`/
  `dapr_scheduler` from `dapr init`), reseed via `dapr/tools/reset.sh` + restart, stop via
  `dapr stop --app-id streamsforge-dapr`. Dapr live-instance tests (plan 025): app `5799`, gRPC `5899`,
  sidecar HTTP `3799`, sidecar gRPC `4799`, dedicated placement `6150` (container `dapr_placement_test`);
  app-id `streamsforge-dapr-test`; Redis logical DB 1 (dev stays on DB 0); Orleans peer for that suite
  `4999`/`5099`, silo `14999`/`34999`. Test instances:
  pick 6xxx–9xxx via `--Http:Port … --Grpc:Port … --DataDir <temp>` and kill them when done. A
  second host on the same machine also needs its own `--Silo:Port`/`--Silo:GatewayPort` (defaults
  11111/30000). Reserved by tests: 9199/9299 (`clients/dotnet`, python, kotlin), 8199/8299
  (`clients/typescript`), 7511/7512 (`tools/soak`), and `orleans/tests/StreamsForge.Chain.Tests`'
  spawned hosts 9399/9499 + 9599/9699 (two-host gRPC chain) and 9799/9899 (restart), silo
  11399/30399, 11599/30599, 11799/30799. TLS tests (plan 024): the Chain project's 6599/6699
  (TLS host), 6999/7099 (TLS startup failure), 8799/8899 + 8999/9099 (TLS two-host chain),
  6799/6899 + 7199/7299 (forwarded headers), silo 16599/36599, 16999/36999, 18799/38799,
  18999/38999, 16799/36799, 17199/37199; and each client SDK's TLS fixture — TypeScript 7199/7299
  (silo 17199/37199; it never runs concurrently with the Chain project, which is why the http pair
  can be shared), .NET 7399/7499 (17399/37399), Python 7599/7699 (17599/37599), Kotlin 7799/7899
  (17799/37799). The four client fixtures all honour `SF_TEST_PUBLISH_DIR` — publish the host ONCE
  and point every suite at it; four concurrent `dotnet publish`es of the same tree on one machine
  is how a 5-minute publish timeout turns into 21 spurious skips.
  Containerized stacks (plan 007): orleans compose `6199`, dapr compose `6399`, admin app `5599` —
  these are the *container* ports; never confuse them with (or bind over) the dev servers above.
- Seeds apply only to an **empty data dir** (`orleans/src/StreamsForge.Host/data/`; delete to
  reseed). Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!`.
- Git: remote `origin` = public `github.com/cotyar/streamsforge`, branch `master`. Commit
  messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Push after stable
  committed waves.

## Build / test / run

One git submodule, `external/ycs` (plan 020): a fresh clone must run `git submodule update --init`
or **both solutions fail to build** — `StreamsForge.Connectors.Crdt.Tests` ProjectReferences it. It is
pinned to the fork's `parity-yjs-13.6.32` branch, NOT `main`; see that project's csproj for why.

```bash
~/.dotnet/dotnet build orleans/StreamsForge.sln
~/.dotnet/dotnet test  orleans/StreamsForge.sln     # 3746 tests — green except the 6 known pre-existing failures (see plans/022); includes StreamsForge.Chain.Tests, which spawns real hosts on 9399–9899 (skips with a named reason if a port is busy)
~/.dotnet/dotnet build dapr/StreamsForge.Dapr.sln
~/.dotnet/dotnet test  dapr/StreamsForge.Dapr.sln   # ~1640 + the Live project (needs `dapr init`, skips with a named reason otherwise)
cd clients/typescript && bun test  # 14 contract + conformance/live-table; boots its own engine on 8199/8299
bun install                       # once, at the REPO ROOT — one workspace, one lockfile
cd web && bun run build           # prebuild compiles @streamsforge/client + @streamsforge/tanstack-db
~/.dotnet/dotnet run --project orleans/src/StreamsForge.Host   # :5199 + :5299
cd dapr && ./tools/run.sh                                      # :5399 (needs `dapr init` done once)
```
Both counts EXCLUDE the 52 live-database tests (`StreamsForge.Connectors.Database.Tests/Integration/**`,
shared by both solutions), which `DockerGate` skips with a stated reason unless a Docker daemon answers
and the backend's image (`postgres:17`, `mcr.microsoft.com/mssql/server:2022-latest`) is already local —
"0 integration tests ran" must never read as "integration passed".

**Kestrel binds IPv4-only, deliberately** (`orleans/src/StreamsForge.Host/Program.cs`). `ListenAnyIP`
binds the IPv6 wildcard in dual-stack mode, and on macOS 26.5 / .NET 10.0.302 an IPv4-mapped accept
throws `System.ArgumentException: The supplied System.Net.SocketAddress is an invalid size…` **unhandled
inside Kestrel's accept loop** — which kills the whole listener ("The connection listener failed to accept
any new connections") and shuts the host down, not just that one connection. Any client resolving
`localhost` to both families can trigger it. The TypeScript client's contract suite is what caught it
(6/14 → 14/14 after the change) and is now a gate for that reason; the cost is that this host does not
answer on IPv6. Revisit only against a runtime that fixes the dual-stack accept path, and re-run that
suite to decide.

**Known time-bounded flakes** (they assert progress within a deadline, so they lose races under
whole-solution parallel load — never under `--filter` on their own): `LoopbackCycleTests`,
`TableRetentionClusterTests`, `TablePersistenceModeClusterTests`, `TableSnapshotMirrorClusterTests`,
`BackfillOnAttachClusterTests`, `ShardedTableClusterTests.RetentionEviction_*`, `ApprovalSweeperTests`,
`ConnectorGrainClusterTests.GetStatusAsync_reflects_a_successful_run` (waits for a connector run's
status to propagate before a deadline),
`WarmUpstreamDiagnosticClusterTests.TableStartedAfterItsTableInputAlreadyHoldsRows_*` (waits for a
warning log line to be emitted before a deadline),
`DuplexSinkTests.SessionThatNeverReturns_FailsAfterThePublishTimeoutRatherThanHanging` and
`HttpSinkClientTests.PublishAsync_*` (added 2026-08-20 during plan 020 wave D — the first asserts the call
returned within `DuplexSinkClient.PublishTimeout + 2s`, an explicit upper deadline; the two
`HttpSinkClientTests` stand up an `HttpListener` on a `GetFreePort()` and wait "within 5s" for the request,
so they carry both a deadline AND a bind race — `GetFreePort` closes its probe listener before the real one
binds, which is a TOCTOU another test can lose them under parallel load. All three pass under `--filter`),
`ShardedTableD2ClusterTests.ShardedTable_KeepsNoPersistedSnapshotMirror` (added to this list 2026-08-28,
observed failing in 2 of 3 whole-solution runs and passing 4/4 under `--filter` in ~0.5s. Its last
assertion reads a SHARD grain's view directly, but the only thing it waits for first is the TABLE view's
row count — and the shard tier is fed asynchronously by the router, so "the table shows 4 rows" does not
imply "the shards have applied them". Under parallel load the shard has not caught up and `view.Found` is
false. Same family as its sibling below — an implicit deadline on the shard tier's write-behind — one rung
weaker still, since here there is no wait at all, only the poll on a different grain),
`ShardedTableD2ClusterTests.ShardedTable_ResumesAsRebuilding_WithoutTheMirrorToDetectItBy` (added
2026-08-20 during plan 020 wave C — its shard-history assertion sits behind a fixed `await Task.Delay(600)`
"to let the write-behind flush persist the HadRows marker", which is the weakest form of deadline: a hard
sleep rather than a poll, so under whole-solution parallel load the second version has not reached the
shard tier yet and `view.History.Sum(...)` reads 1 instead of 2. Passes 10/10 under `--filter`).
`StreamBridgeSourceLifecycleTests.A_newly_upserted_enabled_source_is_relayed_without_waiting_for_the_30s_poll`,
`SourceLateConsumerClusterTests.Pipeline_created_after_a_file_source_already_polled_gets_every_row` and
`StreamsForge.Chain.Tests.HostRestartTests.Url_source_rows_re_land_after_a_host_restart` (all three added
2026-09-04 by plan 023, each observed failing once in a whole-solution run that had the chain project's
spawned hosts and a `TestCluster` competing for the CPU, and passing 2/2 under `--filter` right after. The
first one's 10 s deadline IS its assertion — "relayed well inside the 30 s poll" — so it cannot be widened
past that and stays time-bounded by design. The other two only wait: the late pipeline counts every delivery
of a 700-row file re-parse and the restart test waits for the first post-restart poll, which under load can
lose its HTTP fetch to the CPU-starved in-test listener and take a 30 s backoff before retrying; both
deadlines were widened to 90 s, and if they still lose a whole-solution race the cause is load, not logic).
The same family also lost once each in a later whole-solution run and passed 15/15 alone:
`StreamBridgeSourceLifecycleTests.A_tight_burst_of_six_events_is_relayed_six_for_six` (waits ~10 s for six
relayed hub calls) and the pre-existing `StreamBridgeServiceStartupRaceTests` fact (waits for a seeded
table's first delta). Also, ten `TestClusterPortAllocator.MutexManager` timeouts (tests failing in ~1 ms while BUILDING their
cluster) appeared in the same loaded run and vanished in isolation — that signature is the Orleans test
port allocator's system-wide mutex under contention, never a test's own logic.
Plan 026 wave 1 (2026-09-14) added two more, each observed once in a whole-solution run by the wave's
agent and passing alone: `SourceReplayFromClusterTests.Generator_source_replays_from_a_position` (waits
for a 100 events/s generator to reach position 300 — reached 238 under load inside the original 30 s;
the deadline is now 90 s and the test asserts positions, not timing) and
`TableOverPipelineClusterTests.A_partitioned_table_reads_a_pipeline_through_its_ingest_grains` (polls a
partitioned table's row count to 150 within a deadline; 82 under load, 150/150 alone — the ingest-grain
hop is one more async stage the count has to cross).
Plan 026 wave 2 added `ReplayFromClusterTests.Table_with_replayFrom_over_a_generator_gets_rows_from_that_position`
and `ReplayFromClusterTests.Changing_replayFrom_on_a_running_table_restarts_it`: both count rows after a
2 s quiesce sized for an idle machine, and under a 12-minute whole-solution run the memory-stream pull
agent lagged enough that rows arrived live AND through the replay (plan 023's "ONE GAP"), so the count
overshoots (53 vs 50, 400 vs 100). Pass alone. The same wave-2 whole-solution run also lost, once each
and passing alone afterwards: `BatchReplayGrpcTests.Grpc_SubscribeTable_writes_position_and_flags_truncation_past_retention`
(waits for a 10 005-row file to land as 10 005 table rows — 4 902 after its deadline under load; the
count is the precondition for proving truncation, not the assertion) and the pre-existing
`ConnectorGrainClusterTests.Missing_file_reports_error_status_with_growing_backoff` (waits for a
missing-file source's second failed poll so `ConsecutiveFailures` grows past 1 — a poll cadence race
under load, not logic).
Re-run a failure in isolation before calling it a regression — and report BOTH results, never just the
green one. Nothing else on this list is allowed to grow without a paragraph saying why the test is
time-bounded; a genuinely broken test hiding among "known flakes" is the failure mode this list can cause.
Also: do not pipe a test run through `tail` — it truncates the per-project `Passed!`/`Failed!` lines and
you will not know whether a project ran at all. Grep `^(Passed!|Failed!)` on the full log instead.
Orleans stream-transport knobs (post-005 latency work): `--Streams:Transport push` swaps the
pull-based memory streams for the in-process push bus (`Host/Streaming/PushStream*`, p50 1ms vs
stock 115ms on tableDelta); default `pull` is byte-identical stock Orleans, tunable via
`--Streams:PullPeriodMs` (default 100). `TABLES__FLUSHMS` tunes the epoch flush (P≥2 tables only).
Sharded tables (plan 011 D1, `TableDefinition.ShardBy`, empty = off): `--Shards:IdleSeconds` (default
120) is the shard grain class's activation-collection age — how long an idle key stays resident before
its state is flushed to disk and the grain collected, which is the whole feature; `--Shards:QuantumSeconds`
lowers the silo-wide collection scan interval (Orleans requires it to be strictly smaller than any
collection age, so shortening the idle below ~90s needs it). Orleans-only; Dapr stores `ShardBy` but
refuses to start such a table. Plan 011 D2: a sharded table keeps NO persisted snapshot mirror (the shards
are the durable per-key copy), refuses `MemoryOnly` and refuses being RENAMED (the tier is keyed by name);
`GET /{id}/shards/scan?fenced=true` is an opt-in consistent cut that pauses the tier's ingest for the scan.
Soak shapes: `tools/soak/run-soak.sh --shape orders|instruments`.

Local skills (root `.claude/skills/`, `sf-` prefix) wrap the common workflows: `/sf-run` (both
flavors), `/sf-verify` (both flavors), `/sf-sql`, `/sf-client-gen`, `/sf-config` (catalog
export/import), `/sf-access` (entitlements, approvals, audit), `/sf-federate` (instance discovery,
peer federation, named external endpoints), `/sf-env` (environment isolation — create/select/force-delete).

**Containers, Cloud Run, admin, AI chat** (plan 007): `deploy/orleans/` and `deploy/dapr/` hold
each flavor's Dockerfile(s), `compose.yaml` (host ports 6199/6399), Cloud Run `service.yaml`, and
parameterized `deploy.sh` (`--dry-run` supported; images pinned linux/amd64 — grpc.tools protoc
segfaults under native arm64 Docker). The Dapr stack is self-contained: app+daprd+placement+redis
in one network namespace, no scheduler (timers only). `admin/` (`bun main.ts`, :5599) starts/stops
either containerized stack (or Cloud Run services with `MODE=cloudrun`) and polls `/healthz`.
AI control chat: `POST /api/chat` + SPA "AI Control" page on both flavors, Google Gemini function
calling over the catalog facades — needs `GEMINI_API_KEY` (or `Gemini:ApiKey`), returns a clear 503
without it; capped per login session by `ChatRateLimiter` (`Chat:MaxRequestsPerSession`, default 10,
≤0 disables), 429 past that. Chat logic lives in `shared/StreamsForge.Api/Chat/`.

**JS embeddable server** (`server/`, `@streamsforge/server`, workspace member): the dataset layer
(Z-set table registry + `tableDelta` fan-out + the REST routes `@streamsforge/client` needs) as one
Web-standard `fetch(Request)` handler -- drops into `Bun.serve` (SSE + WebSocket), Hono, or a
Next.js route handler (SSE only; route handlers cannot upgrade). No SQL engine: the "executor" is a
`sf.source(name, rows => table.upsert(...))` handler. The client's `transport: "ws" | "sse"`
(`clients/typescript/src/plain-transport.ts`) speaks to it; never chosen by `"auto"`. Wire
contract + embedding recipes: `server/README.md`. Tests: `bun run --cwd server test` (5).

**Admin CLI + MCP server** (plan 013, `admin/`, zero npm deps like the rest of that folder):
`bun admin/sf.ts <health|login|ls|get|start|stop|create|delete|rows|results|validate|config|api>`
administers a running instance over REST (`SF_URL`, default :5199 — point it at :5399 for Dapr);
`bun admin/mcp.ts` serves the same operations as MCP tools over stdio (hand-written to the spec, no
SDK). Both share `admin/sfclient.ts`. Tests: `bun test admin/` (21).

**Dapr flavor extras**: `dapr/tools/run.sh` starts the sidecar'd host on 5399 (sidecar 3599/4599);
`dapr/tools/reset.sh` SCANs and deletes this app's Redis keys to reseed (the Dapr-flavor equivalent
of deleting Orleans' `data/`); `dapr stop --app-id streamsforge-dapr` stops it. Polyglot processors
(`dapr/processors/{python-enricher,ts-consumer,java-consumer}/`, each with its own `README.md` and
own sidecar/ports) prove the pub/sub contract (`dapr/POLYGLOT.md`) works from outside .NET:
`dapr run --app-id sf-enricher --app-port 8399 --dapr-http-port 3899 --dapr-grpc-port 4899
--resources-path dapr/components -- python3 main.py` (python-enricher), analogous `dapr run`
invocations with their own ports for `ts-consumer` (`bun run main.ts`) and `java-consumer`
(`gradle --no-daemon run`) — see each processor's README for exact ports/env vars.

## Hard rules (learned the expensive way)

1. **Frozen contracts, additive evolution.** `orleans/src/StreamsForge.Engine/PublicApi.cs`,
   existing `StreamsForge.Abstractions` members, and `web/src/api/types.ts` change additively only
   (next free `[Id(n)]`, optional fields). Never edit existing test expectations to make a
   refactor pass — behavior-preserving refactors keep the old tests green *unmodified*.
2. **The Engine stays pure.** No Orleans/Dapr/ASP.NET types inside `StreamsForge.Engine` — it is
   the shared semantic core both runtimes depend on.
3. **Grain reentrancy**: `RegistryGrain` is non-reentrant with a `[MayInterleave]` allowlist.
   Any orchestrator↔worker call cycle deadlocks without allowlisting — check before adding cycles.
4. **Orleans streams serialize payloads**: dictionary subclasses need surrogates (see
   `EventRecordSurrogate`). Memory streams are not a free pass.
5. **Field numbers are forever**: dynamic-protobuf numbering persists in the registry (Active +
   Reserved); both reflection and proto downloads must obtain numbers there, keyed canonically by
   entity **id** even when resolved by name.
6. **SQL fine print**: aggregate JSON with `->` not `->>` (text sums to zero); pipeline-mode
   subqueries must be windowed; `LATEST BY` is table-mode only; `FROM <pipeline>` is table-mode only
   too — a table may read a pipeline by name (the pipeline's `OutputFields` is the relation, so source,
   pipeline and table names share ONE namespace and the create paths refuse a collision), while a
   pipeline still reads sources only, which is what keeps the graph acyclic. A table attaching to a
   pipeline gets **no replay** — pipelines have no ring and no snapshot — so start it before the data
   flows; Dapr has this too as of plan 025 (`CatalogStore`/`TableActor` build the same pipeline-relation
   schema Orleans does, `POST /api/tables/validate` is no longer optimistic there, and the three
   relation-name-collision refusals match). Full list: DESIGN.md §D11.
7. **Branding is neutral**: plain "StreamsForge" wordmark only — every trace of the original
   client's name was removed from the pages, the docs and the plans (2026-08-04/09, pre-open-source);
   do not reintroduce any client name, and never reproduce a client logo graphic. Theme via tokens (`sf.theme`, light default); no raw hex outside the
   sanctioned `--sql-*`/`--chart-*` vars.

## Multi-agent wave discipline (how this repo was built)

- Orchestrator sequences **waves of parallel subagents** (Sonnet, maximum effort) with **strictly
  disjoint file ownership** per concurrent agent; anything shared (csproj, Program.cs, types.ts)
  is pre-assigned to exactly one owner or edited by the orchestrator between waves.
- Engine-exclusive work serializes (one agent owns `StreamsForge.Engine/**` at a time); host/web
  tracks run in parallel with it.
- Every agent brief includes verification gates: build + **full** test suite (including the other
  flavor's regression suite once `shared/` exists) + live checks against a self-started instance
  on isolated ports, killed afterward. Commit (and push) between waves, one logical change per
  commit.
- Contracts that two concurrent agents must meet in the middle (stream envelopes, grain
  interfaces, DTO shapes) are pinned verbatim in both prompts, or pre-built by the orchestrator.
