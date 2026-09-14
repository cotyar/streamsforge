# Adding a transport

A **transport** is how bytes get into StreamsForge (a source kind) or out of it (a sink kind). NATS is the
reference implementation of both directions; this document is the recipe for the next one.

Built in today: `nats` inbound and outbound, and (plan 012) a `file` sink that appends rows to a local
file as CSV or NDJSON — the egress twin of the `file`/`folder` source kinds, and the proof that this seam
holds for a destination that is not a broker at all. `FileSinkTransport`/`FileSinkClient` are worth
reading next to the NATS pair: same interfaces, same fire-and-forget contract, a third of the code.

The design goal is stated as a test, not as a promise:
[`TransportRegistryTests`](orleans/tests/StreamsForge.Host.Tests/TransportRegistryTests.cs) registers a
transport the repository has never heard of and asserts the platform validates it, masks its credentials,
drives its messages into rows, and offers it in the console. If a hardcoded per-kind branch ever creeps back
in, those tests fail.

---

## What you write, and what you don't

| You write | You do **not** touch |
|---|---|
| A config class in `shared/StreamsForge.Contracts/ConnectorModels.cs` | `SecretsMasker` — secrets are found by `[Secret]` |
| One `IInboundTransport` and/or one `ISinkTransport` | `SourceValidation` — transports validate themselves |
| One line in `InboundTransports` / `SinkTransports` | `ConnectorGrain` (Orleans) and `ConnectorActor` (Dapr) |
| — | `NatsPublisherService` / `NatsSinkPublisherService` |
| — | Anything under `web/` — the console builds its form from your descriptor |

Before plan 010 this list was about fourteen places across both runtime flavors. The parts that were only
*incidentally* NATS-specific — the reconnect/backoff loop, payload→row mapping, ack discipline, secret
masking, eligibility filtering, form rendering — are now shared.

---

## Inbound (a source kind)

### 1. The config contract

`shared/StreamsForge.Contracts/ConnectorModels.cs`. Additive only: the **next free `[Id(n)]`**, never a
renumber (field numbers are forever — see `CLAUDE.md`).

```csharp
public static class SourceKinds
{
    // …
    public const string Rv = "tibco.rv";
}

public sealed class ConnectorConfig
{
    // …existing [Id(0)]–[Id(6)]…
    [Id(7)] public RvSubConfig? Rv { get; set; }
}

[GenerateSerializer]
public sealed class RvSubConfig
{
    [Id(0)] public string Daemon { get; set; } = "";
    [Id(1)] public string Subject { get; set; } = "";
    [Id(2)] public string Format { get; set; } = FileFormats.JsonArray;
    [Id(3)] public string? Username { get; set; }
    [Id(4)] [Secret] public string? Password { get; set; }   // ← the ONLY thing masking needs
}
```

**`[Secret]` is the whole secrets story.** It makes the value mask as `***` on every read path (GET, list,
config export), and makes a written `***` restore the stored value on PUT. Getting this wrong used to mean a
credential exported in plaintext, silently; now the failure mode is a compile-visible missing attribute, and
[`SecretWalkTests`](orleans/tests/StreamsForge.Host.Tests/SecretWalkTests.cs) fails if a known slot loses it.

Do **not** mark identifiers (`Username`, `Url`, `Subject`) — masking an identifier makes the form unusable and
the export unreadable.

### 2. The transport

```csharp
public sealed class RvInboundTransport : IInboundTransport
{
    public string Kind => SourceKinds.Rv;

    // Which payload format the shared parse path should use for this source.
    public string FormatOf(SourceDefinition def) => Config(def).Format;

    // Accumulates one message per problem; empty means accepted. Never throws.
    public void Validate(SourceDefinition def, List<string> errors)
    {
        var cfg = def.Connector?.Rv;
        if (cfg is null) { errors.Add("kind 'tibco.rv' requires connector.rv"); return; }
        if (string.IsNullOrWhiteSpace(cfg.Daemon)) errors.Add("connector.rv.daemon is required");
        if (string.IsNullOrWhiteSpace(cfg.Subject)) errors.Add("connector.rv.subject is required");
    }

    // One connection ATTEMPT. Called again on every reconnect — do not make it re-enterable.
    public IInboundSubscription Open(SourceDefinition def) => new RvSubscription(Config(def));

    public TransportDescriptor Describe() => /* see "The console form" below */;

    private static RvSubConfig Config(SourceDefinition def) =>
        def.Connector?.Rv ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'tibco.rv' but no rv config");
}

internal sealed class RvSubscription(RvSubConfig cfg) : IInboundSubscription
{
    public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
    {
        // Connect here (not in Open), then yield until ct fires or the broker ends the stream.
        // Third element is the ack callback, or null when the transport has no redelivery to acknowledge.
        await foreach (var m in DialAndConsume(cfg, ct))
            yield return new InboundMessage(m.Subject, m.Bytes, AckAsync: null);
    }

    public ValueTask DisposeAsync() => /* close the connection */;
}
```

**Contract you get for free**, all in [`SubscriberCore`](shared/StreamsForge.AppCore/Transports/SubscriberCore.cs):

- Reconnect forever with the D-E backoff (`min(30s · 2^(k-1), 15 min)`); a *clean* end of the stream
  reconnects immediately with no backoff and resets the failure counter.
- Payload → row through the one shared path a polled HTTP body uses: parse by `FormatOf`, extract per
  `MappingSpec`, coerce per `SourceDefinition.OnCoercionFailure`, dedup per `MappingSpec.DedupKeyField`,
  stamp `_source`/`_ts`.
- A bad message never tears down the subscription — it becomes a status error and the loop continues. Only
  an exception out of `SubscribeAsync` counts as a connection failure.
- Ack is called only on a clean outcome, so a message rejected by coercion is redelivered if your transport
  has redelivery.
- **Stream replay (plan 026), free of any code in your transport.** Whatever `ConnectorGrain` publishes
  onto the source's stream also lands in that source's `ReplayLog` behind the shared `ReplayGate` —
  the same one-line dispatch through `InboundTransports.Find(kind)` above is what makes this automatic.
  A late-attaching consumer, a `GET /api/sources/{name}/replay` status read, and a `replayFrom: {seq:…}`
  gRPC/SignalR subscription all work for your kind the moment it registers, with nothing transport-
  specific to opt into. Opting a *definition* of your kind into `replay: { persist: true }` similarly
  needs no transport code — the disk-backed tier lives at the grain/log layer, entirely below `Open`.

**Contract you must honor:** `SubscribeAsync` must respect `ct` at every await, and must not swallow
connection errors — throwing is how you ask for a reconnect.

### 3. Register it

`shared/StreamsForge.AppCore/Transports/InboundTransports.cs`:

```csharp
private static readonly List<IInboundTransport> Registered = [new NatsInboundTransport(), new RvInboundTransport()];
```

That is the entire wiring. Both connector drivers already dispatch through `InboundTransports.Find(kind)`.

---

## Outbound (a sink kind)

Same shape, two interfaces:

```csharp
public sealed class RvSinkTransport : ISinkTransport
{
    public string Kind => SinkKinds.Rv;

    // "Enough config to attempt a connection." A half-filled sink is NOT configured — returning true here
    // would produce failure counters and log noise for a sink nobody has finished setting up.
    public bool IsConfigured(SinkSpec spec) =>
        spec.Rv is { } r && !string.IsNullOrWhiteSpace(r.Daemon) && !string.IsNullOrWhiteSpace(r.Subject);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new RvSinkClient(spec.Rv!, entityKind, entityName, onFailure);

    public TransportDescriptor Describe() => /* below */;
}
```

`ISinkClient.PublishAsync` has a **hard contract**: it must never throw and never block the caller past its
own timeout. Both publisher services await it with no try/catch, deliberately — a sink that stalls would
stall the pipeline it is attached to. Count failures internally (`SinkPublishCounters`), report them through
the throttled `onFailure` callback, and drop. Copy
[`NatsSinkClient`](shared/StreamsForge.AppCore/Sinks/NatsSinkClient.cs) — its per-publish linked
`CancellationTokenSource` is what makes "never blocks" true against a disconnected broker.

Register in `SinkTransports.Registered`, same as above.

### TLS (`nats` inbound and outbound)

`NatsSubConfig.Tls` / `NatsPubConfig.Tls` (both `NatsTlsConfig?`, null by default) add four optional
fields — `caFile`, `certFile`, `keyFile` (all paths on the **host's** filesystem, not file contents,
the same convention `FileSinkConfig.Path` uses) and `insecureSkipVerify` — layered on top of, not
instead of, the `tls://` URL scheme: a `tls://` server URL with a publicly-trusted cert needs none of
this. Set the CA/cert/key paths for a private CA and/or mutual TLS. `insecureSkipVerify` disables
server certificate validation entirely and is **dev-only** — never set it against a broker that
matters.

---

## What environment isolation (plan 021) does to a transport

Most transports need to do nothing at all for this. `nats`, `http`, `db` and `file` all name something
**outside this process** — a subject, a URL, a database table, a path on the host filesystem — and an
environment has no opinion about any of those: qualifying them would silently rename an operator's Kafka
subject or their database table out from under them. `ISinkTransport`'s and `IInboundTransport`'s
signatures are unchanged by plan 021 for exactly this reason, so an out-of-tree transport written against
plan 010's SPI still compiles.

**`loopback` and `duplex` are the two exceptions, because both name a CATALOG ENTITY rather than an
external endpoint.** A `loopback` sink's `targetSourceName` names a generator source that is meant to
receive the sink's rows back as new events (`LoopbackHub`); a `duplex` sink's `sourceName` names a
`fix-duplex` source owning a live FIX session (`DuplexSessions`). Both registries are keyed by the
entity's **runtime key** — `EnvKeys.Qualify(environment, name)` — because that is what plan 021 qualified
every name-keyed grain/actor by. Before wave 2 of that plan, the sink's config held the bare catalog name
and nothing translated it, and the bug this produced was silent and it wrote across the environment
boundary: a table in `staging` with a `loopback` sink to `feed` published to the bare key `feed`, which is
exactly the key `default`'s own generator is attached at — `staging`'s rows landed in `default`'s source,
and the publish **reported success**. Not a missing feature; a working cross-environment write, found
while chasing an unrelated loopback inconsistency.

`SinkEnvironmentScoping.Scope` (`shared/StreamsForge.AppCore/Sinks/SinkEnvironmentScoping.cs`) is the fix:
called at sink-client construction, it qualifies `Loopback.TargetSourceName` / `Duplex.SourceName` by the
entity's own environment and returns a cloned `SinkSpec` — never written back to the catalog, the same
rule plan 016 gives `@name` endpoints, so an export from `staging` stays importable into `prod`. The
default environment gets the identical `SinkSpec` instance back, so nothing about an untouched deployment
allocates or changes.

**If your new sink kind ever names a catalog entity by string** (not a URL, not a subject, not a table) —
the way `loopback` and `duplex` do — route it through `SinkEnvironmentScoping.Scope` the same way, or it
will reproduce exactly this leak the moment more than one environment exists.

**`fix-duplex` sources register under the environment-qualified key too** — `ConnectorGrain`/`ConnectorActor`
activate at `EnvKeys.Qualify(environment, sourceName)` like every other connector, so `DuplexSessions.Find`
is looked up by the qualified name from the scoped `SinkSpec` above, not the bare one a `duplex` sink's
config still carries.

---

## Polled sources (a database, or anything else pull-shaped)

Everything above is **push**-shaped: something else decides when a message exists, and `SubscribeAsync`
yields until it throws. A database table is the opposite — nothing arrives until you ask — and plan 014
added a second seam for exactly that: [`IPolledTransport`](shared/StreamsForge.AppCore/Transports/IPolledTransport.cs),
implemented today by `StreamsForge.Connectors.Database` (postgres, mssql, postgres-cdc, mssql-cdc).

**It is a *sibling* of `IInboundTransport`, not a generalization of it — on purpose.** `IInboundTransport.Open`
hands back an async enumerable, which means the polling loop (and its cursor — the one piece of state that
must survive anything) would have to live *inside the subscription instance*. That is in memory, and it is
lost on every silo recycle, actor deactivation and rebalance — precisely the thing that must not happen to a
cursor. `PollAsync(def, cursor, ct) -> PolledBatch` puts the loop in the *driver* instead, which already
persists its state once per cycle, so the cursor rides along for free instead of needing a subscription
object with its own lifecycle.

```csharp
public interface IPolledTransport
{
    string Kind { get; }
    void Validate(SourceDefinition def, List<string> errors);
    Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct);
    TransportDescriptor Describe();
}

public sealed record PolledBatch(IReadOnlyList<Dictionary<string, object?>> Rows, string? Cursor, bool HasMore);
```

- **`Cursor` is opaque.** The transport mints it, the driver persists it, nothing in between parses it — an
  LSN, a composite `(ts,id)` string and a plain bigint all fit without the platform ever learning what any
  of them mean. `null` means "leave the persisted cursor exactly as it was" (what an empty poll returns);
  it is **never** "reset to the beginning" — a transport that wants to restart a source has to say so with
  an actual value, not by returning null and hoping.
- **`HasMore` means "re-arm right now"** instead of waiting for the schedule. That single bit is what makes
  a paged backfill resumable: a large initial read pages across successive *driver* cycles, each persisting
  its own cursor, so a restart mid-backfill resumes where it stopped rather than starting over. Paging
  inside one `PollAsync` call would put those intermediate cursors back in memory, which is the exact
  failure this whole seam exists to avoid.
- **The load-bearing rule: a failed cycle keeps the OLD cursor** — enforced once, in
  [`PolledSourceCore`](shared/StreamsForge.AppCore/Transports/PolledSourceCore.cs), not by each transport,
  because a transport bug is exactly the case the rule has to protect against, and a rule enforced by the
  code that might be buggy protects nothing. **Throwing from `PollAsync` is therefore a normal, expected
  outcome** — a database is down far more often than a config is wrong — not something your transport needs
  to catch and convert into an empty batch. `PolledSourceCore` turns the exception into a reported error
  with the incoming cursor handed back unchanged, so the same rows are re-read next cycle rather than
  skipped.
- **No `FormatOf`, no ledger, no mapping** — three deliberate divergences from `IInboundTransport`. A result
  set is already structured, so there is nothing to parse. There is no file whose mtime answers "did this
  change" — the cursor replaces that question outright. And for a row source the SELECT list (or the
  publication, for CDC) **is** the mapping; a JSONPath layer on top would be a second way to say the same
  thing, free to disagree with the first the moment a column is renamed in one and not the other. That is
  what `TransportDescriptor.Mapping = false` tells the console: stop offering a mapping editor for this kind.
- **`ISchemaProbe` is an optional capability**, not a second interface every polled transport must implement.
  `POST /api/transports/{kind}/probe` looks for it on the registered transport and 400s when it is absent —
  which is how schema discovery reaches the console without `StreamsForge.Api` learning what a database (or
  anything else pull-shaped) actually is; it knows a probe returns fields and diagnostic notes, and nothing
  further. `TransportDescriptor.CanProbe` is what lets the console render the "Discover" button honestly
  instead of hopefully.

**The four kinds that exist today, and the one-line difference between the two pairs:** `postgres` and
`mssql` poll a table or your own query on a durable, monotonic cursor **column** — cheap, and it never sees
a transaction that commits after a later-timestamped one already moved the watermark past it (the honest
argument for the next pair). `postgres-cdc` and `mssql-cdc` read the source database's own change log
instead of polling anything — see below.

### Change data capture

**Using either kind — standalone with no StreamsForge server, or as a source inside StreamsForge, with the
worked examples and every operational hazard below spelled out in full — is [`docs/cdc.md`](docs/cdc.md).**
This section stays the *recipe* (what you'd change to add a third native CDC dialect); that document is the
*user guide* for the two that already exist.

The native CDC pair is `postgres-cdc` ([`PgCdcSource`](shared/StreamsForge.Connectors.Database/PgCdcSource.cs))
and `mssql-cdc` ([`MsSqlCdcSource`](shared/StreamsForge.Connectors.Database/MsSqlCdcSource.cs)), both
`IPolledTransport` like their cursor-polled siblings — CDC is still pull-shaped from this platform's point
of view, one cycle at a time, even though the *source* database is doing the pushing internally.

- **Postgres**: logical replication over a **slot + publication**, read via `Npgsql.Replication`'s pgoutput
  decoder. **SQL Server**: the built-in capture tables, read via `cdc.fn_cdc_get_all_changes_*` on a
  `binary(10)` LSN.

**The operational hazards, stated where an operator will actually read them (the descriptor `Help` text, not
just here):**

- An **undrained Postgres slot pins WAL until the *source* database's disk fills** — not StreamsForge's disk,
  the database being read from. `max_slot_wal_keep_size` is the server-side safety valve; it is not a
  substitute for keeping the source running.
- **SQL Server CDC retention defaults to 3 days.** A source left stopped longer than that has permanently
  lost whatever retention already discarded — the next cycle fails loudly (a retention-breach check, not a
  silent skip) rather than quietly resuming from wherever it can.
- **`REPLICA IDENTITY FULL`** is what makes a Postgres `DELETE` carry more than its key columns. Without it,
  a delete event is genuinely partial (key columns only) — not fabricated, not dropped, just partial, and
  both the native reader and the Debezium envelope path (below) treat it identically.
- An **unchanged TOASTed column arrives as the sentinel `__debezium_unavailable_value`**, not its real
  content — Postgres never wrote the value into this change record in the first place, so there is nothing
  to decode; a consumer that treats the sentinel as real data corrupts silently.

**Delivery is at-least-once**, same ceiling as the cursor-polled kinds, and **a batch always ends on a
transaction boundary** in both dialects — rows from a transaction whose COMMIT (Postgres) or whose read
this cycle could not prove complete (SQL Server's `TOP`-capped read, re-read bounded to its own
`__$start_lsn` with no `TOP` when needed) are held back rather than emitted split or over-eagerly confirmed.
`BatchSize` is therefore a **target** for a cycle's read, never a hard ceiling on what one batch can emit —
a transaction larger than `BatchSize` is delivered whole, over budget, once the bounded re-read resolves it.
Emitting a truncated group and advancing the cursor past it would be silent, permanent data loss, which is
the failure this rule exists to make impossible.

**Backfill is asymmetric between the two dialects, and each says so in its own `Help` text:**

- `postgres-cdc` **refuses `Snapshot` outright** — a replication slot carries no history from before its own
  creation, so there is nothing to snapshot. Backfill with the polled `postgres` kind first, then switch the
  source to `postgres-cdc` to tail changes from where the backfill left off.
- `mssql-cdc`'s `Snapshot` means **"replay whatever the capture table still retains"**, not a full-table
  snapshot — CDC retention is finite (see the 3-day default above), so `Snapshot` here can only ever replay
  what has not aged out yet. For a true backfill, run the polled `mssql` kind first.

**`MappingSpec.Envelope = "debezium"` still exists**, and is still the right route for a database this
connector does not speak natively — MySQL, Oracle, MongoDB via Debezium Server emitting into a NATS source.
The native kinds are an *addition* to that path, not a replacement for it: they exist because this connector
already speaks Postgres and SQL Server natively, so a slot/capture-table reader was cheap; nothing about it
extends to a database this project has no client for.

**The honest limit, restated so it does not get lost between the two paths:** a StreamsForge source is an
append-only `EventRecord` stream — `_weight` on an *inbound* row is just a column, a value like any other,
whichever path stamped it. The Engine's Z-set weights that make a table a genuine multiset are computed
*from* table SQL, not carried in from ingress. So neither `postgres-cdc`/`mssql-cdc` nor the Debezium
envelope path retracts the row a delete removed; it arrives as one more event (`_op = "d"`, `_weight = -1`
on the Debezium path) sitting in the stream next to every insert and update that came before it. The working
pattern is `LATEST BY <key>` + `WHERE _op <> 'd'` on the downstream table — which **hides** a deleted key
from query results but does **not free it**: the tombstone event, and everything before it, is still sitting
in the source's history. See [`CdcEnvelope`](shared/StreamsForge.AppCore/Connectors/Mapping/CdcEnvelope.cs)'s
class doc for the canonical wording — this section restates it rather than diverging from it.

---

## FIX

> **Installation note (plan 022):** the `fix`/`fix-duplex` session **source and sink kinds** below now
> ship as the `StreamsForge.Plugins.Fix` server plugin under `plugins/`, not a host `ProjectReference` —
> see `PLUGINS.md`. The `fix` wire **format** (`FixParser`, used by any `url`/`file`/`folder`/`nats`
> source) stays in `StreamsForge.AppCore` and needs no plugin; only the live session kinds do. Everything
> below about the format, the session, and `fix-duplex` is otherwise unchanged by that move.

Plan 018 adds two things that are useful separately and better together: a wire **format** (`fix`) that
any `url`/`file`/`folder`/`nats` source can name, and a receive-only session **source kind** (also spelled
`fix` — a coincidence of both being named after the protocol, not a conflict; a `SourceDefinition.Kind`
and a `FileFormats` value are two different registries) that speaks the session and declares
`FormatOf => FileFormats.Fix`, so it reuses the format's parser and the whole shared mapping/coercion/dedup
path with nothing of its own. Had the session converted FIX to JSON internally instead, it would have
worked identically and a FIX log on disk would still be unreadable by this platform — the format existing
independently is the point.

### The `fix` format

`format: "fix"` sits alongside `ndjson`/`json`/`csv`, parsed by
[`FixParser`](shared/StreamsForge.AppCore/Connectors/Formats/FixParser.cs) into the same
one-`JsonElement`-per-item shape the other three parsers already produce.

- **tag=value, delimiter sniffed.** A real session speaks SOH (`\x01`); logs, tickets and test fixtures
  use `|` or `^`, because SOH doesn't paste into a text editor. The delimiter is sniffed once per input,
  following `FormatParsers.SniffDelimiter`'s own doctrine: highest count over the first frame wins, a tie
  goes to the earlier candidate, nothing counted falls back to SOH.
- **No FIX dictionary.** Names and types come from one static table covering the common FIX 4.2/4.4/5.0
  tag set — tag numbers are globally unique across FIX versions by design (tag 35 is `MsgType` in every
  version from 4.0 through 5.0), so one table is correct rather than a version-specific compromise. A tag
  this table doesn't know becomes `"tag<N>"`, a plain JSON string keyed to it. An operator who wants more
  has declared field types (`ConnectorRowCoercion`) and 009-C1's SQL conversion functions downstream —
  exactly what CSV's untyped columns already lean on.
- **Values are typed from that table, never sniffed.** CSV sniffs `long`/`double`/`bool` because it has
  nothing better to go on; FIX has a spec that says tag 44 is a price and tag 55 is a symbol, so sniffing
  would silently turn `55=123` into the number 123. Known numeric tags become a JSON number, the FIX
  `Boolean` type (exactly `"Y"`/`"N"`) becomes `true`/`false`, and every other tag — known or unknown —
  stays a string.
- **Repeating groups become nested JSON arrays.** `NoMDEntries=2` followed by two entries becomes
  `"MDEntries": [{…}, {…}]` — **this is the most useful thing about the format**: it is what lets
  `MappingSpec.ItemsPath = "$.MDEntries[*]"` fan one market-data snapshot out into one row per quote,
  using the exact same JSONPath machinery every other source's mapping already runs. The framing needs no
  dictionary: a counter tag from a known table opens a group, the tag right after it is the group's
  delimiter tag, the first entry establishes the group's tag set, and the group ends at the first tag
  outside that set (or the standard trailer — see the ceilings below).
- **Length-prefixed fields are read by byte count.** `RawDataLength(95)`/`RawData(96)` and its siblings
  (90/91, 212/213, 350/351, 354/355, 358/359, 360/361, 362/363, 364/365) carry a character count precisely
  because the value may contain the delimiter; the parser takes the paired data tag verbatim for exactly
  that many characters, delimiters and `=` signs included, instead of splitting on the next delimiter it
  finds. This is FIX's answer to CSV's quoted field, and skipping it corrupts exactly the messages
  (raw/encoded/XML payloads) that are hardest to debug afterward.

**The two ceilings, stated plainly — read these before leaning on group framing:**

- **Dictionary-free group framing infers a group's field set from its first entry.** A later entry that
  carries a field the first entry omitted terminates *that later entry* right at the tag the first entry
  never had — the trailing fields land on the *parent* object, not the entry they visually belong to.
- **A single-entry group is bounded only by the next delimiter tag, a repeated tag, or the standard
  trailer (93/89/10).** A single-entry group that is *not* the last thing at its nesting level absorbs
  whatever follows it, because there is no second entry to reveal where it actually ends.

Upgrade path for both: a real FIX dictionary, which knows a group's field set without needing a second
entry to reveal it. Neither is a corner case found later by accident — `FixParserTests` names both
explicitly, and the code carries a `// ponytail:` comment at the exact line that hits the first one.

**`fix` is ingress-only.** There is no FIX sink: `FileSinkTransport.Describe()`'s format list stays
`[csv, ndjson]`, and `FileSinkClient` has no FIX twin — writing FIX with no session to number the messages
produces something no counterparty would accept.

### The `fix` source kind

A persistent FIX session — market data, drop-copy — as an `IInboundTransport`, living out of the core in
[`shared/StreamsForge.Connectors.Fix`](shared/StreamsForge.Connectors.Fix) on `QuickFIXn.Core` 1.14.1,
registered the same way TIBCO Rendezvous would be (see "Transports whose client library cannot ship in
this repo", below): `QuickFIXn.Core` is a dependency of this one out-of-core project, not of
`StreamsForge.AppCore`. This platform is always the FIX **initiator**, dialing out to the config's
`host`/`port`; the counterparty is always the acceptor.

**Receive-only, on purpose — order entry is a separate plan.** A FIX session that both sends orders and
receives execution reports is [`plans/019-fix-order-entry.md`](plans/019-fix-order-entry.md), and it is a
different plan rather than a later wave of this one, in one sentence: one FIX session spans this
platform's two independent registries (sources and sinks), `ISinkClient`'s never-throw fire-and-forget
contract is wrong for a `NewOrderSingle`, and sequence-number persistence becomes a cluster-singleton
correctness requirement the moment this side originates messages instead of only receiving them.

Config fields (`FixSourceConfig`, `connector.fix` in a `SourceDefinition`):

| Field | Purpose |
|---|---|
| `host` / `port` | Counterparty to dial. Always required; this side is always the initiator. |
| `senderCompId` / `targetCompId` | This session's own CompID and the counterparty's. |
| `beginString` | FIX version header (`FIX.4.0`–`FIX.4.4`, `FIXT.1.1`). Selects the header only — see `UseDataDictionary=N` below. |
| `username` / `password` | Optional; sent as tags 553/554 inside the Logon(A) message. QuickFIX/n has no built-in credential exchange, so this is this platform's own addition. `password` is `[Secret]` and masks to `***` on read and export. |
| `heartBtIntSeconds` | Session heartbeat interval, seconds. Must be > 0. |
| `resetOnLogon` | Reset sequence numbers to 1 every logon (default `true`). |
| `storePath` | Empty (default) = in-memory sequence-number store. Non-empty = file-backed. See the hazards below. |
| `useSsl` | Wraps the socket in TLS. Deferred beyond this bare flag: client certificates, CA pinning. |
| `onLogon` | Raw FIX text, one message per line, sent right after logon succeeds. See below. |
| `msgTypes` | Comma-separated MsgType (tag 35) include-filter, e.g. `W,X`. Empty = every application message. |
| `queueCapacity` | Capacity of the bounded, drop-oldest bridge queue. Default 10000. See the hazards below. |

**`UseDataDictionary=N` is set unconditionally**, so **no `FIX44.xml` (or any version's dictionary) is a
deployment artifact.** QuickFIX/n does the session layer and no message validation, and hands the
application message over intact — `Message.ConstructString()` returns the raw SOH-delimited wire string,
which is exactly the `byte[]` the format parser above then parses. `beginString` only selects which
version header this session claims, never a schema to validate against.

**`onLogon` is raw FIX text, not a request builder.** A market-data session must SEND something (a
`MarketDataRequest`, a `SecurityListRequest`, …) right after logon to receive anything at all — this field
holds one raw FIX message per line, delimiter-sniffed the same way the format parser sniffs a payload (SOH,
`|` or `^`, whichever the operator actually typed). No templating, no request/response correlation, no
resubscribe-on-reject: a typed request builder is a plan-019-sized decision, not a field on this class. A
send failure here fails the whole connection attempt — silence would be the worst possible symptom.

**Operational hazards — read these before enabling the kind:**

- **The bridge from QuickFIX/n's session thread is a bounded, `DropOldest` channel.** `FromApp` is a
  synchronous callback on the FIX session's own thread; blocking it would apply backpressure to the
  session itself and eventually trip the counterparty's heartbeat timeout, a worse failure than dropping.
  `DropOldest` is **right for market data** (a stale quote is worthless) and **wrong for drop-copy**,
  where every message matters. `queueCapacity` sets the bound; a running count of drops
  (`FixBridgeApplication.Dropped`) is logged to stderr on every increment rather than swallowed, and is a
  best-effort operator signal (it can race the channel's own reader by one) rather than an exact ledger.
- **`storePath` empty = in-memory store + `ResetOnLogon`, which is right for market data** — resending
  yesterday's quotes is worse than not resending them, so a clean slate every logon is the default. Set
  `storePath` for a drop-copy session that must not lose its place across restarts, and **in a container
  it must be a mounted volume** — the same requirement the `file` sink's `path` field already carries.
- **`AckAsync` is always null: at-most-once into the platform.** QuickFIX/n's sequence layer acknowledges
  a *session* — the counterparty need not resend an in-sequence message — which is not the same thing as
  this platform having processed the row into a table. Claiming otherwise would be exactly the lie a
  non-null `AckAsync` is not allowed to tell.
- **Session-level traffic never reaches the row path.** Logon/Heartbeat/TestRequest/ResendRequest/
  SequenceReset/Logout are consumed by QuickFIX/n's own session layer and never surface to the application
  callback, so "receive-only" costs nothing to enforce. `msgTypes` filters only the application messages
  that do reach it.

**Worked example** — a market-data source definition (`POST /api/sources` body) subscribing to EUR/USD
top-of-book and fanning its `MDEntries` group into one row per quote:

```json
{
  "name": "eurusd-md",
  "description": "EUR/USD top-of-book, FIX 4.4 market data, receive-only",
  "kind": "fix",
  "connector": {
    "fix": {
      "host": "fix.venue.example.com",
      "port": 9880,
      "senderCompId": "STREAMSFORGE",
      "targetCompId": "VENUE",
      "beginString": "FIX.4.4",
      "heartBtIntSeconds": 30,
      "resetOnLogon": true,
      "storePath": "",
      "useSsl": false,
      "onLogon": "35=V|262=1|263=1|264=1|265=1|267=2|269=0|269=1|146=1|55=EUR/USD",
      "msgTypes": "W",
      "queueCapacity": 10000
    },
    "mapping": {
      "itemsPath": "$.MDEntries[*]",
      "fields": [
        { "sourcePath": "MDEntryType", "field": { "name": "side", "type": "String" } },
        { "sourcePath": "MDEntryPx", "field": { "name": "price", "type": "Double" } },
        { "sourcePath": "MDEntrySize", "field": { "name": "size", "type": "Double" } }
      ]
    }
  },
  "onCoercionFailure": "Null"
}
```

The `35=V` line in `onLogon` is a `MarketDataRequest`: `262=1` (MDReqID), `263=1` (Snapshot+Updates),
`264=1` (MarketDepth top-of-book), `265=1` (IncrementalRefresh), `267=2`+`269=0`+`269=1` (NoMDEntryTypes=2:
Bid, Offer), `146=1`+`55=EUR/USD` (NoRelatedSym=1: the symbol). The venue's `35=W` reply then arrives with
a `NoMDEntries` group, and `itemsPath` fans it into one row per entry with `side`/`price`/`size` pulled out
of each.

### `fix-duplex` — a bidirectional session (plan 019): configuring the two halves

`fix-duplex` (plan 019, [`shared/StreamsForge.Connectors.Fix/FixDuplexTransport.cs`](shared/StreamsForge.Connectors.Fix/FixDuplexTransport.cs))
is one live FIX session with an inbound half (execution reports, rejects, anything the venue sends back)
and an outbound half (orders, cancels, replaces) — the third transport seam after `IInboundTransport` and
`IPolledTransport`, `IDuplexTransport`. It is declared as **two entities that meet in the middle**:

1. **A source** of kind `fix-duplex`, carrying the same `FixSourceConfig` the receive-only `fix` kind uses
   (host/port/CompIDs/beginString/credentials/…) plus the persistence rules below, which are stricter here
   than for `fix`. This source's inbound half behaves exactly like a `fix` source — same format
   (`FileFormats.Fix`), same mapping, same row path into whatever table/pipeline reads from it.
2. **A `duplex` sink**, on any `PipelineDefinition`/`TableDefinition`, naming that source by
   `duplex.sourceName`. The sink holds no connection of its own — every send resolves the live session by
   name, through `DuplexSessions.Find`, and forwards. Publishing a row to a table/pipeline with this sink
   attached is what "sending an order" means: `SELECT * FROM my_orders` into a table whose `sinks` list
   includes `{"kind": "duplex", "duplex": {"sourceName": "fixoe"}}` sends every row that lands in that
   table out over `fixoe`'s session.

**Why two entities and not one.** The whole point (plan 019 D2) is that tearing down the sink costs
nothing — it owns no connection — while tearing down the session would re-logon an order session
mid-flight. A pipeline/table's `Sinks` list is re-signed and rebuilt on ANY field edit, including an
unrelated one (`SinkSelection.Signature`, a content hash of the active sink list, re-checked every 30s);
if the session lived there, every such edit would cost a logon. It does not, because the session lives in
the source's connector driver (the Orleans `ConnectorGrain` / Dapr `ConnectorActor` that already owns the
inbound half) and the sink only ever asks it, by name, whether it is there.

**Row → FIX mapping, and the `MsgType`-from-row rule.** A row reaching the outbound half needs a `MsgType`
column (tag 35, e.g. `"D"` for `NewOrderSingle`) — **the message type comes from the ROW, not from
`FixSourceConfig`**, because one duplex source's outbound half plausibly carries more than one message
type across its lifetime (a `NewOrderSingle` here, an `OrderCancelRequest` there), and a per-source config
default cannot express that. Every other column is looked up in a curated name → tag table
(`FixRowMapper.TagByName`, the outbound mirror of the inbound format's tag table) covering the order/
execution, market-data and party fields an outbound message plausibly carries; **a column with no known
tag refuses the WHOLE row** rather than dropping just that column — see the reserved-column gotcha below
for the one shape of "unknown column" the mapper does NOT refuse. `TransactTime` (tag 60) is stamped when
absent on a `NewOrderSingle`/`OrderCancelRequest`/`OrderCancelReplaceRequest` row and **never overwritten**
when the row already carries one — QuickFIX/n's `Session` layer stamps `SendingTime` (tag 52) for every
outbound message but does not stamp tag 60, and FIX 4.4 requires it on those three message types, so
leaving it unstamped produced a wire message venues reject (found in review, not by a wave — see
`plans/019-fix-order-entry.md`'s "What actually landed").

**The curated required-field table, and its ceiling.** `FixRequiredFields` gates three message types —
`NewOrderSingle` (`ClOrdID`/`Symbol`/`Side`/`OrdType`/`OrderQty`), `OrderCancelRequest`
(`OrigClOrdID`/`ClOrdID`/`Symbol`/`Side`), `OrderCancelReplaceRequest` (a cancel's fields plus `OrdType`/
`OrderQty`) — refusing a row missing any of them before it ever reaches the wire, naming both the field
and its tag number in the refusal. **A `MsgType` this table has no entry for is not gated at all** — this
is not a real FIX dictionary (plan 018's "no dictionary" decision holds outbound too, just not
unconditionally: this one curated table is the exception), so a message type outside these three sails
through with whatever columns the row happened to carry. Required-field validation runs AFTER the
readiness check (an order to a session that is not logged on is refused for that reason regardless of the
row's own completeness — the operator's next action is the same either way) but ClOrdID generation (next
paragraph) and mapping both run BEFORE it.

**Opt-in `ClOrdID` generation, and what its uniqueness is — and is not.** `generateClOrdId` (default
`false`) fills a missing `ClOrdID` with a fresh `Guid.NewGuid()` — off by default because a row missing
`ClOrdID` is more useful refused than silently completed; a caller-supplied `ClOrdID` is **never**
overwritten even when generation is on. A GENERATED id is unique for all practical purposes with no
tracked registry (2^122 of them, and a fresh one every connection attempt rather than a per-session
counter, so a reconnect needs no persisted base token). **A CALLER-SUPPLIED id is not checked for
uniqueness at all** — there is no in-memory or persisted table of previously-sent `ClOrdID`s to check
against, matching plan 019 D7's "the platform has no notion of an entity that is amended rather than
appended." Two rows with the same caller-supplied `ClOrdID`, in one batch or across a reconnect, are both
sent as given; a venue reject for the duplicate is an ordinary first-class inbound outcome (an
`ExecutionReport` or `OrderCancelReject` row), not something this layer prevents.

**Gotcha, found live rather than by a unit test: every row a duplex sink actually forwards carries
platform-reserved columns, and the mapper must skip them.** A hand-built `Dictionary<string, object?>` in
a unit test carries only the columns the test wrote. A REAL row — the only kind a `duplex` sink ever
forwards in production — always carries `_ts` and `_source` (every row the engine produces has them;
`StreamsForge.Engine.PublicApi.EventRecord`'s own doc comment: "Reserved keys: `_ts`… `_source`"), and a
row sourced from a TABLE's delta stream additionally carries `_weight` (`SinkStepGuard.RowOf` stamps it
so a retraction is not indistinguishable from an insert downstream). Before wave 019-I2's live drop-copy
check — the first check to send an order through a real `TableDefinition`'s SQL output rather than call
`FixDuplexSession.SendAsync` directly with a hand-built row — `FixRowMapper.TryBuildMessage` refused
**every single one** of these rows, on `_ts` first (pipeline-sourced) or `_weight` (table-sourced, after
`_ts`/`_source` were fixed), because an unmapped column refuses the whole row. `FixRowMapper` now skips
`_ts`/`_source`/`_weight` exactly like it already skips the `MsgType` column itself — a reserved platform
column is not a business field with an opinion about FIX tags. The lesson generalizes: **a check that
calls the session directly with a hand-built row cannot see anything the platform itself stamps onto a
row before a sink ever touches it** — see this file's own note in `plans/019-fix-order-entry.md`'s "What
actually landed" for the fuller account.

**Worked example** — an "orders" table whose rows are sent out over a `fixoe` `fix-duplex` source:

```json
// POST /api/sources — the duplex session itself
{
  "name": "fixoe",
  "kind": "fix-duplex",
  "connector": {
    "fix": {
      "host": "oms.venue.example.com", "port": 9881,
      "senderCompId": "STREAMSFORGE", "targetCompId": "VENUE",
      "beginString": "FIX.4.4", "heartBtIntSeconds": 30,
      "resetOnLogon": false, "storePath": "/var/lib/streamsforge/fix/fixoe.store",
      "generateClOrdId": false
    },
    "mapping": { "itemsPath": "$", "fields": [
      { "field": { "name": "ClOrdID", "type": "String" } },
      { "field": { "name": "ExecID", "type": "String" } },
      { "field": { "name": "OrdStatus", "type": "String" } }
    ] }
  }
}
```
```json
// POST /api/tables — orders flow in from "new_orders" and out over fixoe's session
{
  "name": "orders_sent",
  "sql": "SELECT * FROM new_orders",
  "sinks": [ { "kind": "duplex", "duplex": { "sourceName": "fixoe" } } ]
}
```
A row `{"MsgType": "D", "ClOrdID": "ORD-1", "Symbol": "EUR/USD", "Side": "1", "OrdType": "2", "OrderQty":
1000000, "Price": 1.2345}` landing in `new_orders` sends a `NewOrderSingle` over `fixoe`; the venue's
`ExecutionReport` arrives back on `fixoe`'s inbound half and can be joined to the order that caused it —
by `ClOrdID` — in ordinary platform SQL: `SELECT o.ClOrdID FROM orders_sent o INNER JOIN execs e ON
o.ClOrdID = e.ClOrdID` (`execs` being a table `SELECT * FROM fixoe`) is the whole of plan 019 D7's
correlation table, verified live end-to-end in `plans/019-fix-order-entry.md`'s "What actually landed".

### `fix-duplex` — mandatory sequence-number persistence, and how to recover (plan 019 D5)

`fix-duplex` (plan 019, [`shared/StreamsForge.Connectors.Fix/FixDuplexTransport.cs`](shared/StreamsForge.Connectors.Fix/FixDuplexTransport.cs))
is the same `FixSourceConfig` as the receive-only `fix` kind above, dialed the same way, but it also sends —
and that changes what `storePath`/`resetOnLogon` are allowed to be. Above, `storePath` empty + `resetOnLogon`
true is **the right default for market data**: resending yesterday's quotes is worse than not resending them,
so a clean slate every logon is correct and a lost sequence count costs at worst some re-sent ticks. **None of
that reasoning survives contact with an order session.** The store is not an optimization there — it is this
platform's only record of what it told the venue it sent. So `Validate()` refuses, for `fix-duplex` only:

- an empty (or whitespace) `storePath` — an in-memory store is refused outright, not merely warned about;
- `resetOnLogon: true`, independently of `storePath` — a durable store that gets thrown away every logon
  buys nothing, so the two failure modes are reported together rather than one quietly undoing the other;
- a `storePath` that is not an absolute path — a relative path resolves against the process's working
  directory, which no deployment of this platform promises is stable;
- a `storePath` under `/tmp` or `/var/tmp` — the one ephemeral-location pattern this process can actually
  detect cheaply, from the string alone, with no filesystem access. **Read the ceiling in that message
  literally**: this process cannot see the volume mounted behind *any* path, so a path that does *not* start
  with `/tmp` has not been confirmed durable either — it has only not been caught by the one check available
  from inside a running process. Writability is deliberately not checked: touching the filesystem during
  validation buys nothing (a `tmpfs` mount is writable and exactly as ephemeral as `/tmp`), and an unwritable
  store fails loudly on the first connection attempt instead of silently.

**What happens, concretely, when a container restart loses the store.** `storePath` pointing at container-local
storage (the platform-level mistake the checks above catch what they can of) means the next process start finds
no store file. QuickFIX/n's `FileStoreFactory` initializes a fresh one at sequence 1 on both sides of the
session — MsgSeqNum 1 out, and this side now *expects* MsgSeqNum 1 in. The venue's acceptor did not restart:
its own store still holds the real sequence numbers from before the crash. Two things happen on the next logon,
depending on which side notices first:

1. This side logs on claiming MsgSeqNum 1. The venue's session layer sees a **sequence number lower than
   expected** and, per the FIX spec, rejects the Logon (or logs on and immediately issues a
   `35=2` `ResendRequest` for the gap, depending on the venue's own leniency) rather than accepting a stream
   that appears to have gone backwards.
2. If the venue's session instead notices the mismatch mid-stream, it sends a `35=2` `ResendRequest` for the
   range `[expected, infinity)`. **This platform cannot answer it.** The messages it is being asked to
   re-send never happened in this process's lifetime — they were sent by the *previous* process, whose
   sequence state is exactly what was lost. `FixDuplexSession` has no order log to replay from; plan 019
   explicitly does not build one (see "Not in this plan" below).

**What this looks like from the venue's side**, because the fix is coordinating with them, not guessing: they
see a counterparty that either failed to log on at all (sequence-too-low rejection), or logged on and then
either went silent in response to a `ResendRequest` or answered it with `35=4` `SequenceReset-GapFill`
messages this platform never sent (because nothing on this side generated them) — in FIX terms, indistinguishable
from a counterparty whose session state is simply wrong. Venues that are strict reject the session outright and
require a manual sequence reset coordinated out-of-band (phone, chat, a support ticket) before trading resumes.
Venues that are lenient may auto-reset **their own side** to match whatever this platform claims, which is the
dangerous case: it silently accepts an under-counted sequence and any messages between the true prior sequence
and the claimed "1" are now unaccounted for on both sides, not just missing from this platform's log.

**Recovery, in order:**

1. **Do not restart-and-hope.** Restarting again with the same broken `storePath` reproduces the same mismatch;
   restarting with `resetOnLogon: true` set "to get unstuck" is exactly the setting `Validate` refuses for this
   kind, for this reason.
2. **Check whether the store file is actually gone, or just not where `storePath` says.** A path that pointed
   at container-local storage before a redeploy is often recoverable if the *old* container/volume still
   exists — mount it back and start `storePath` pointing at the recovered file before touching sequence numbers
   at all.
3. **If the store is genuinely gone, this is a sequence reset, coordinated with the counterparty — not a
   restart.** Contact the venue (or, for an internal/test counterparty, whoever administers its acceptor) and
   agree a `35=4` `SequenceReset` (hard reset, not gap-fill) to a mutually agreed number on both sides — usually
   the venue's next-expected-inbound and next-expected-outbound, reset to a value both sides log on
   with cleanly next. Do this *before* restarting this platform's session; logging on before the counterparty
   has reset its own expectation reproduces the original mismatch against a moving target.
4. **Only after the counterparty has confirmed its side is reset**, restart this platform's `fix-duplex` source
   with `storePath` pointing at a fresh, empty, durable location, and `resetOnLogon` still `false` — the fresh
   file's sequence numbers become the new baseline for both sides from that logon forward.
5. **Reconcile what actually happened in the gap.** Whatever `ClOrdID`s the platform believes it sent versus
   what the venue's execution reports (or drop-copy) confirm receiving is exactly the correlation table plan
   019 D7 describes — run it for the affected time window before assuming any order in flight during the loss
   either went through or didn't.

**What this platform does not do, stated plainly**: it is not an OMS, it does not persist orders (only FIX
session sequence state, and only when `storePath` is set), and a lost store is not something it can reconstruct
from its own state — there is no order log anywhere in this design for it to replay from. The correlation table
in D7 tells you what the venue confirms happened; it cannot tell you what this platform *meant* to send if the
store that recorded that intent is the thing that was lost. That is what makes the check above refuse an
in-memory store outright rather than merely recommending against it.

---

## The console form

`Describe()` returns a [`TransportDescriptor`](shared/StreamsForge.AppCore/Transports/TransportDescriptor.cs),
served from `GET /api/transports` (Viewer). The SPA renders it with one generic component
([`TransportConfigEditor.tsx`](web/src/components/sources/TransportConfigEditor.tsx)) — there is no
per-transport React code, and adding one requires no change under `web/` at all.

```csharp
public TransportDescriptor Describe() => new()
{
    Kind = SourceKinds.Rv,
    Label = "TIBCO Rendezvous",
    Help = "A persistent subject subscription — not a poll schedule, so this kind ignores Schedule.",
    ConfigProperty = "rv",              // ← the ConnectorConfig / SinkSpec property holding your config
    Groups =
    [
        new TransportGroup { Key = "auth", Label = "Credentials" },
    ],
    Fields =
    [
        new TransportField { Key = "daemon", Label = "Daemon", Required = true, Mono = true, Placeholder = "tcp:7500" },
        new TransportField { Key = "subject", Label = "Subject", Required = true, Mono = true },
        new TransportField
        {
            Key = "format", Label = "Payload format", Type = TransportFieldTypes.Select,
            Options = [FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv], Default = FileFormats.JsonArray,
        },
        new TransportField { Key = "username", Label = "Username", Group = "auth" },
        new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret, Group = "auth" },
    ],
};
```

Field types: `string`, `secret`, `number`, `bool`, `select`.

- `ConfigProperty` must match the property name on `ConnectorConfig` / `SinkSpec`, camelCased as it goes on
  the wire. The console reads and writes `connector[configProperty]` generically.
- Every `secret` field **must** correspond to a `[Secret]` property. A test enforces the two agree — a field
  that renders masked but exports in plaintext is the exact bug the attribute exists to prevent.
- A group with `Optional = true` and an `ObjectKey` renders with an on/off switch and writes `null` for the
  whole nested object when off. That is how "core NATS vs a JetStream consumer" is expressible; use it for
  any opt-in feature block that must be *absent*, not blank.
- Groups without an `ObjectKey` are purely visual — their fields live flat on the config object.

**The descriptor deliberately has no conditional-visibility or cross-field rules.** Validation lives in
`Validate()` and the modal already renders the messages it returns; a rules language here would be a second
validator in TypeScript that drifts from the first.

---

## An out-of-tree kind: install, don't fork

Everything above assumes the transport lives in this repo. It does not have to. A kind can ship as a
library nobody here references — **one DLL in `plugins/`, and (optionally) one ES module in
`ui-plugins/`** — and the platform picks up both at startup with no rebuild and no config beyond the
files themselves.

> **This is also how three IN-TREE kinds ship today** (plan 022): the Quant pricing scalars, the
> `fix`/`fix-duplex` transports, and (Orleans only) the `crdt` source's grain-backed facade are each
> built as a plugin under this repo's own `plugins/` directory rather than referenced from a host
> project — merged into one DLL apiece and copied next to the host binaries automatically by the
> build, so a normal checkout behaves exactly as if they were still linked in. `PLUGINS.md` at the repo
> root is the full guide: the two host hooks (`Register()` / `IStreamsForgeWebPlugin`), a decision table
> for which one to use, worked examples for all three in-tree plugins plus an out-of-tree template, the
> ILRepack merge rule and why, and the runtime failure modes when a plugin is missing. This section
> keeps the transport-authoring recipe; read `PLUGINS.md` for the installation mechanics around it.

```csharp
// YourCompany.Orion.dll, dropped in <host binaries>/plugins/ (or wherever `Plugins:Path` points)
public sealed class OrionPlugin : IStreamsForgePlugin      // StreamsForge.AppCore.Plugins
{
    public string Name => "Orion connector 1.2.0";
    public void Register() => InboundTransports.Register(new OrionInboundTransport());
}
```

The loader reports one line per outcome at startup (`[plugins] plugin 'Orion connector 1.2.0' … registered`)
and **never throws**: a plugin that fails to load, constructs badly, or tries to register a kind that is
already taken is skipped with a reason, because a third-party file must not keep the host from starting.
Plugins load AFTER the built-in registrations, so a plugin cannot shadow a built-in kind — it loses the
duplicate-kind race and says so. Assemblies load into the host's DEFAULT load context (they must share its
`IInboundTransport` type to register at all), so a plugin's own dependency versions are not isolated from
the host's.

### Config with no config class: the `settings` bag

The one thing an out-of-tree kind cannot do is add a property to `ConnectorConfig`/`SinkSpec` — those live
in `StreamsForge.Contracts`, in this repo, and every typed config class is there for a reason
(`SecretWalk` only recurses into types from that assembly, so a config class declared elsewhere would
export its password in plaintext). The bag closes that from the other side:

```csharp
public TransportDescriptor Describe() => new()
{
    Kind = "orion",
    ConfigProperty = "settings",                    // ← the open bag, not a typed property
    Fields =
    [
        new TransportField { Key = "environment", Label = "Environment", Required = true },
        new TransportField { Key = "subEnvironment", Label = "Sub-environment" },   // a 4th dimension costs nothing
        new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
    ],
};

public void Validate(SourceDefinition def, List<string> errors)
{
    var s = def.Connector?.Settings;
    SettingsBag.Require(s, "environment", "Environment", errors);
    var sub = SettingsBag.GetOrNull(s, "subEnvironment");        // absent and blank both read as null
    var timeout = SettingsBag.GetInt(s, "timeoutMs", 5000);      // unparseable → the fallback, never a throw
}
```

`ConnectorConfig.Settings` / `SinkSpec.Settings` is a `Dictionary<string, string>` that the platform
stores, exports, imports and renders without knowing a single key. What it buys, and what it costs:

- **Adding a field is not a schema change.** A new key in a bag spends no `[Id(n)]`, breaks no older
  document, and needs no coordination with this repo — which is the whole point when a kind's config has
  several orthogonal dimensions (environment × location × sub-environment × …) that keep growing.
- **Secrets still mask**, but by DESCRIPTOR, not by attribute: a field declared
  `Type = TransportFieldTypes.Secret` is masked as `"***"` on every read path and follows the same
  "sending `***` back keeps the stored value" rule as every typed credential. `SettingsBag`'s readers are
  in `StreamsForge.AppCore.Transports`.
- **A kind nobody registered masks its WHOLE bag.** With no descriptor there is no way to tell a hostname
  from a password, and the platform fails closed — so an export taken on a host where the plugin is not
  installed is unhelpful rather than a leak.
- **Everything is a string.** `number`/`bool` fields are written as their plain spelling by the console
  and parsed by `SettingsBag.GetInt`/`GetBool`. `@name` endpoint references work as they do anywhere
  else — call `NamedEndpoints.Resolve` on the value at connect time, exactly like a typed config does.
- **The ceiling: no nested optional group.** A descriptor group with an `ObjectKey` (a nullable nested
  object — "core NATS vs a JetStream consumer") cannot be expressed in a flat bag. A kind that genuinely
  needs one needs a typed class in `StreamsForge.Contracts`, i.e. a PR here.

### Schema discovery works for push kinds too

`ISchemaProbe` is optional for ANY transport, polled or pushed: implement it and
`POST /api/transports/{kind}/probe` reaches it (`SourceSchemaService.ProbeAsync` checks
`PolledTransports` first, then `InboundTransports`), the console renders **Discover schema** off
`CanProbe`, and a probe that throws comes back as a diagnostic on a 200 rather than a 500.

---

## A specialized console editor (UI plugin)

For the kinds the generic form can't express — a topic browser, a connection tester, a query builder — a
library outside this repo can ship its own React editor and have the console load it. Nothing under `web/`
changes, and no StreamsForge assembly is rebuilt.

A plugin is **one file** in the host's `ui-plugins/` directory (`<host output dir>/ui-plugins/`, or
wherever `Ui:PluginsPath` points): either an ES module (`.js`/`.mjs`) or **one TypeScript file**
(`.ts`/`.tsx`) — single file only, no `import` statements, relative or package (an automatic-JSX-runtime
TSX file would emit one, which is why TSX plugins write classic JSX against
`const React = window.streamsforge.react` instead). A `.ts`/`.tsx` file is served as plain text and
transpiled **in the browser** at load time via a lazily-downloaded `sucrase` (only fetched when a
listed plugin actually needs it); a `.js`/`.mjs` file imports directly, no transpile step. Ship it from
a connector package as content copied to the output directory and a `PackageReference` installs the UI
along with the transport. The console fetches `GET /api/ui-plugins` before its first render (the
listing is `Cache-Control: no-store` and every URL carries a `?v=` version, so a plain reload always
sees a changed file — no hard reload needed), imports each module, and the module registers itself:

```js
// ui-plugins/rv.js — see web/plugins-example/example-nats.js for a complete one.
const { react, registerTransportEditor } = window.streamsforge
registerTransportEditor('rv', RvEditor, 'inbound')  // omit 'inbound' to serve the sink half too
```

`RvEditor` gets exactly the props the built-in editor gets — `{ descriptor, value, onChange, isEdit,
disabled, idPrefix, direction, draft, onSuggest }` — and replaces `TransportConfigEditor`'s output for
that kind, in the source modal and the sinks editor alike (they both render through that one component,
which is why one registration covers both). `draft` (`{ name, description, fields, tags }`) is a
read-only view of the entity being edited, and `onSuggest(patch)` proposes a change to those same four
fields: `name`/`description` land only while the user's own field is still blank (a plugin may call it
from an effect as often as it likes and can never clobber typed text), `fields` replaces the schema
editor's rows exactly like "Discover schema" does, and `tags` are unioned. Both props are `undefined` in
the sinks editor — a sink has no name/description. Author typings for either language live in
`web/plugins-example/streamsforge-plugin.d.ts`; `web/plugins-example/example-nats.tsx` is a worked TSX
example (the plain-JS `example-nats.js` stays too).

`window.streamsforge` also hands over what the console already has, so a plugin never pays for a second
copy of it (`apiVersion` is `3`; feature-detect with `(window.streamsforge?.apiVersion ?? 0) >= 3`):

| Member | What it is |
| --- | --- |
| `react` | The console's own React. A bundled second copy breaks hooks. |
| `api` | Authenticated REST — `get`/`post`/`put`/`del`, bearer token AND the selected environment header, `ApiError.status` on failure. |
| `live` | `subscribeTable` / `subscribeSource` / `subscribePipeline` on the console's ONE SignalR connection. A plugin that opened its own client would mean a second socket, a second auth handshake and a second subscription for the same rows. |
| `loadLiveTables()` | Lazily resolves `{ createCollection, createLiveQueryCollection, streamsForgeCollectionOptions, connect }` — TanStack DB over a StreamsForge table, for a plugin that wants query/join on top of raw deltas. Dynamically imported, so a console that loads no plugin never downloads it; `connect()` is memoized and uses this console's origin + session token. |

Rules that bite:

- **`onChange` replaces the whole config object.** Spread the previous value; a config carries fields your
  editor doesn't show (an optional group's nested object, a secret) and a bare `{[key]: v}` deletes them.
- **Secrets stay secrets-lite**: on `isEdit` a stored value reads back as `***`, and sending `***`
  unchanged keeps it. An editor that "helpfully" clears that field wipes the credential on save.
- **`window.streamsforge.react` is the console's own React** — use it rather than bundling a second copy,
  which would break hooks. `apiVersion` is bumped only if those props change shape (2 → 3 added
  `draft`/`onSuggest`).
- **Validation still lives in `Validate()` on the backend.** A plugin that also validates is a second
  validator that drifts, exactly as above.
- A plugin that throws while rendering takes down its own panel, not the console (`PluginErrorBoundary`);
  one that fails to import is logged and skipped. Neither loses the stored configuration.
- **The listing and the files are anonymous** — the console loads them before login. They are front-end
  assets served to every browser that can reach the console; put nothing else in that directory.

Registration is per `kind`, and that is the only seam: a plugin cannot add pages, routes, or catalog
behavior. A kind with no plugin keeps the descriptor form — which remains the answer for almost every
transport.

---

## Named external endpoints (`@name`)

Plan 016 wave 6. Any config field holding a host, URL or connection string can be authored as `@name`
instead of a literal — the **whole** value, never a substring (`nats://user@host:4222` is left exactly as
written; only a value that is *entirely* `@` + a name counts). `NamedEndpoints.Resolve(value)`
(`shared/StreamsForge.AppCore/Discovery/NamedEndpoints.cs`) turns it into whatever this environment has
configured under `Endpoints:<name>` — read once at host startup from `--Endpoints:<name>=…` /
`Endpoints__NAME` / an `Endpoints` object in `appsettings.json`, never from the catalog — and throws a
message naming both the missing endpoint and every name this environment does know. `TryResolve` is the
same lookup without throwing, for the one caller (`ConfigImportService`) that has to keep going and report
a warning instead of aborting.

**A new transport that dials out wires this in itself.** It is not free from `IInboundTransport` /
`ISinkTransport` / `IPolledTransport` the way secret masking or eligibility filtering is, because only the
transport knows which of its own fields are endpoint-shaped. The six existing call sites are the pattern to
copy — one `NamedEndpoints.Resolve(...)` call each, right where the literal would otherwise be handed to the
client library, so a `@name` reference is indistinguishable from a resolved literal by the time it reaches
the wire: the `url` poll driver (both flavors' connector grain/actor, on `ConnectorConfig.Url`),
`NatsConnectionSettings` (`Url`), `HttpSinkClient` (`Url`), `GrpcPeerResolver` (`Address`/`RestAddress`,
on the branch that has no `Peer` set — see below), both SQL dialects' `CreateConnection` (`Host`/
`ConnectionString`), and the FIX session builder (`Host`).

**Resolve at connect time, never at validate/save time.** A document imported into an environment that
doesn't (yet) have the name configured must still import — only dialing fails. Resolving early (inside
`Validate()`, say) would reject an otherwise-portable document on the wrong environment for the wrong
reason.

**Not masked.** `GET /api/meta/endpoints` (Viewer) returns every configured name's *value*, in the clear —
a connection string put behind a name carries its credential exactly as configured. Say so in your new
kind's own docs if its endpoint-shaped field is the sort that might hold one.

An `@name` endpoint (and a `Discovery:Peers` entry) may resolve to an `https://` value just as readily as
an `http://` one — nothing here is scheme-specific — and a private CA that value's certificate chains to
comes from `Tls:TrustedCaPath`, not from anything configured on the endpoint or peer entry itself. See the
next section.

### Outbound TLS trust

`Tls:TrustedCaPath` and `Tls:AcceptAnyCertificate` (`shared/StreamsForge.AppCore/Net/OutboundTls.cs`) are
the one place this platform decides whether it trusts a remote server's certificate, and they apply to
*every* outbound HTTPS/gRPC connection the host makes — a `url` source's fetch, an `http` sink's publish,
a `grpc` source's federated subscribe, a peer probe, OpenAPI schema derivation — not per-transport
settings each connector would otherwise have to grow on its own. `Tls:TrustedCaPath` is a PEM file (one or
more certificates) that **extends** the system trust store rather than replacing it: a publicly-trusted
`https://` endpoint needs no configuration here at all, and a certificate signed by the configured CA
still fails on a plain hostname mismatch — "our CA signed it" and "it's the host we asked for" are kept as
two different questions on purpose, so a custom root can never launder an impersonating peer. Configured
once at host startup, before anything dials out, because the outbound `HttpClient`s that use it are
process-wide singletons that capture their handler on first use — a `Tls:TrustedCaPath` set after that
point would silently apply to only some of them.

`Tls:AcceptAnyCertificate=true` disables outbound verification altogether. It exists for a self-signed
peer in local development and logs a startup warning for exactly that reason — never set it against
anything that matters. On Linux, `SSL_CERT_FILE`/`SSL_CERT_DIR` are the OpenSSL-level equivalent for
whatever sits below this platform's own handler (e.g. a native library a plugin brings in). This is the
*outbound* axis only; the host's own server-side certificate (what a browser or another instance sees
when it dials *this* host) is a separate setting — see `orleans/docs/index.html` § TLS.

---

## Transports whose client library cannot ship in this repo

TIBCO Rendezvous is the motivating case: `TIBCO.Rendezvous` is not on public NuGet — it ships with a
licensed Rendezvous installation and wraps a native library. Putting it in `shared/StreamsForge.AppCore`
would make the main build require a license.

Put it in its own project that neither solution references, and register from host startup — either by
editing that host (when the project is in this repo but unreferenced by the main build):

```csharp
// orleans/src/StreamsForge.Host/Program.cs — before the host starts serving.
InboundTransports.Register(new RvInboundTransport());
SinkTransports.Register(new RvSinkTransport());
```

…or, when it is not in this repo at all, by shipping an `IStreamsForgePlugin` and dropping the DLL in
`plugins/` — see [An out-of-tree kind](#an-out-of-tree-kind-install-dont-fork) above, which is the same
registration one file later.

Registration must happen before any source starts. A duplicate `Kind` throws rather than silently shadowing
a built-in.

---

## What is deliberately *not* pluggable

**The `grpc` source kind stays its own branch** in both drivers. It subscribes to a remote StreamsForge and
decodes typed protobuf frames against a schema fetched by reflection — it never asks "what format is this
payload", which is the question this seam is built around. Bending it into `IInboundTransport` would mean
widening the interface for exactly one implementation. `IInboundTransport` is the subject/topic +
opaque-payload family (NATS today; RV, MQTT, AMQP, Kafka next); `IPolledTransport` is the separate,
pull-shaped family covered above (postgres/mssql/postgres-cdc/mssql-cdc today) — between the two, most
future ingress kinds fit one or the other.

**Plan 016 wave 5 added `GrpcSubConfig.Peer`**: a configured peer NAME, resolved by `GrpcPeerResolver` at
each (re)connect and winning over `Address`/`RestAddress` when set — see
[Federated addressing](orleans/docs/index.html#discovery-federation) for the discovery side of this. It
lives entirely inside the branch above; the seam itself — still its own thing, still not
`IInboundTransport` — is unchanged.

**`crdt` (plan 020) isn't here either, and for a sharper reason than `grpc`'s.** A Yjs update is not
bytes in a named format — it's a delta against durable, per-document state, and it only produces rows
after being merged into that state. `IInboundTransport`'s seam is "bytes → rows through a named format
parser" (`FormatOf` returns `"json"`/`"csv"`/`"fix"`, and `ConnectorPollCycle.ExecuteMessage` does the
rest); bending a CRDT merge into that shape would mean a transport that secretly owns persistence — the
exact second extraction path with its own subtly different semantics this seam exists to prevent (plan
020 D3). So `crdt` dispatches to its own grain (`CrdtDocGrain`, the same shape as `IGeneratorGrain`), not
through `InboundTransports`/`ConnectorGrain` at all, and Orleans-only for now (plan 020 D9 — the Dapr
flavor stores the kind and refuses to start it). **Installation note (plan 022):** `CrdtDocGrain` and the
Orleans facade that talks to it now ship as the `StreamsForge.Plugins.Crdt` server plugin under
`plugins/`, replacing the core's disabled default facade via `IStreamsForgeWebPlugin.ConfigureServices`
— see `PLUGINS.md`. The grain interface, the facade contract, the HTTP routes and everything else this
paragraph already describes stay in the core untouched by that move. Full documentation, including the
config shape, the three-stamp deletion convention, the replay route, and the reconciliation pattern for
what a CRDT cannot enforce, is in [orleans/docs/index.html#crdt](orleans/docs/index.html#crdt) — this
document stays the transport recipe, and a CRDT document isn't a transport.

**The registries are static lists, not DI discovery.** Assembly scanning would buy nothing — transports are
compile-time known — and both connector drivers are constructed by runtime machinery (an Orleans grain, a
Dapr actor) whose container is not the host's; injecting a registry into a grain has already broken this
repo's test cluster once. `Register()` covers the out-of-tree case above — and the `plugins/` loader does
not weaken this: it discovers FILES an operator installed, never transport TYPES to infer registration
from. What runs is still one explicit `Register()` call, written by the plugin's own author.

---

## Checklist

- [ ] Config class with the next free `[Id(n)]`, `[Secret]` on every credential, nothing else marked
- [ ] Every host/URL/connection-string field routed through `NamedEndpoints.Resolve` at connect time (see
      [Named external endpoints](#named-external-endpoints-name) above) — skip only if the kind genuinely
      dials nothing external
- [ ] `IInboundTransport` / `ISinkTransport` implemented, including `Describe()` — **or**, for a pull-shaped
      kind, `IPolledTransport` (plus `ISchemaProbe` if it can discover its own schema)
- [ ] Registered in `InboundTransports` / `SinkTransports` / `PolledTransports` (or, out-of-tree, from an
      `IStreamsForgePlugin` in `plugins/` — and then config in the `settings` bag rather than a new property
      on `ConnectorConfig`)
- [ ] `~/.dotnet/dotnet test orleans/StreamsForge.sln` and `dapr/StreamsForge.Dapr.sln` — both suites green,
      **no existing test file modified**
- [ ] `cd web && bun run build` — should need no source change; it is a check that nothing regressed
- [ ] Only if the generic form genuinely can't express the kind: a UI plugin (see
      [A specialized console editor](#a-specialized-console-editor-ui-plugin)) — dropped into `ui-plugins/`,
      loaded live, and checked to preserve secrets and untouched config fields on save
- [ ] Live check on an isolated port (6xxx–9xxx, `--Http:Port … --Grpc:Port … --DataDir <temp>`, killed
      afterwards): the kind appears in `GET /api/transports`, an invalid config is rejected with your
      messages, credentials read back as `***`, a masked PUT round-trip preserves them, and the source arms
      and degrades rather than crashing against an unreachable broker
- [ ] For a polled kind specifically: a cycle that fails (kill the source mid-poll, or point it at an
      unreachable endpoint) leaves the persisted cursor untouched rather than advancing or nulling it — the
      one property `PolledSourceCore` exists to guarantee, and the one worth checking live rather than
      trusting the unit tests alone
