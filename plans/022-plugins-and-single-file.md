# Plan 022 — Server plugins & single-file publish

**Status: wave 0–1 DONE, wave 2 (CRDT plugin) landing now** — waves 0 and 1 are committed
(`0661349`, `59abcf7`); wave 2 moves `CrdtDocGrain`/`OrleansCrdtFacade` into a fourth in-tree plugin,
`StreamsForge.Plugins.Crdt`, and is the one part of this plan still in flight as this document is
written. The final pass on this file (exact test counts, wave 2's actual commit hash, anything wave 2
changed that this draft could not yet see) happens once wave 2's own report lands — see the note at
the top of the "What actually landed" table below.

**Depends on**: nothing hard. Builds directly on the existing `settings`-bag / descriptor-driven
out-of-tree-kind mechanism from plan 010/014 (`TRANSPORTS.md`'s "An out-of-tree kind" section) — this
plan is what lets three features **already in this repo** install through that same mechanism instead
of being linked into the host, and adds the publish story that makes the result deployable as one file.

## Why

Three features that are genuinely optional at runtime — the Quant pricing scalars (QLNet, ~7 MB), the
FIX session transports (QuickFIXn), and CRDT edge sync's Orleans grain (Ycs) — were nonetheless
compiled directly into both hosts, so every deployment carried all three whether or not it used any of
them, and there was no way to ship a *fourth* such feature (a real customer connector, say) without a
PR to this repo. Separately, a StreamsForge deployment had no path to "one file, no repo checkout, no
`dotnet` SDK on the target machine" — it was always a `dotnet run`/`dotnet publish -f-d` (framework
dependent) or a container.

This plan gives both problems the same answer: an explicit, minimal plugin contract
(`IStreamsForgePlugin`, optionally `IStreamsForgeWebPlugin`) a DLL implements to install itself into a
running host, proven by moving three real in-tree features onto it — and a single-file publish path
that carries whatever plugins are present along with it, because a deployable that can't also carry its
optional features would only be half the story.

## Decisions, and what they cost

### D1. Two hook tiers, and the split lives at the Api/AppCore boundary, not inside one interface

`IStreamsForgePlugin` (`shared/StreamsForge.AppCore/Plugins/StreamsForgePlugins.cs`) is the whole
contract a plugin that only touches process-wide registries needs: a name and a `Register()` call.
`IStreamsForgeWebPlugin : IStreamsForgePlugin` (`shared/StreamsForge.Api/Plugins/IStreamsForgeWebPlugin.cs`)
adds two more, default-no-op hooks — `ConfigureServices`/`MapEndpoints` — for a plugin that needs the
host itself.

**Why two interfaces instead of one with optional members**: `ConfigureServices`/`MapEndpoints` take
`IServiceCollection`/`IEndpointRouteBuilder` — ASP.NET types. Hard rule 2 forbids ASP.NET types inside
`StreamsForge.AppCore`, so a single interface would either violate that rule or force AppCore to take an
ASP.NET Core dependency it has never had. Splitting the interface at the same Api/AppCore boundary the
rest of the codebase already respects means AppCore's plugin loader (`StreamsForgePlugins.LoadFrom`,
which every plugin — Quant, Fix, Crdt, and anything out-of-tree — goes through) has zero ASP.NET
references, and only a host that actually maps `IStreamsForgeWebPlugin` instances takes on the Api-side
driver (`StreamsForgePluginHosting`).

**Cost**: a plugin author has to know which interface to implement, and picking the wrong one (declaring
`IStreamsForgeWebPlugin` for a plugin that never overrides either hook) costs nothing functionally but
is a tell that the author didn't need the extra tier. `PLUGINS.md`'s decision table exists specifically
to make that choice legible without reading both interfaces' source.

### D2. Convention over interface for UI modules — no member on `IStreamsForgePlugin`

A plugin that wants to ship a console UI module does **not** declare it through the interface (no
`GetUiModules()` method, no marker attribute). Instead, `StreamsForgePlugins.LoadFrom` scans the loaded
assembly's manifest resources for anything under `ui-plugins/` with a `.js`/`.mjs` extension, purely by
naming convention (`<EmbeddedResource Include="ui-plugins/*.js" LogicalName="ui-plugins/%(Filename)%(Extension)" />`
in the plugin's own csproj).

**Why**: the resource is fully discoverable from the assembly alone — there is nothing a method would
tell the loader that scanning `GetManifestResourceNames()` doesn't already answer, and adding a method
would be one more thing every plugin author has to implement (even as a no-op) for a feature most
plugins don't use. The cost is that UI-module discovery only fires for an assembly that registered **at
least one** `IStreamsForgePlugin` — see D2a.

### D2a. UI-module scanning is gated on "this assembly registered a plugin", not run over every DLL

`ScanUiModules` only runs for an assembly that just activated at least one `IStreamsForgePlugin` type
(`LoadAssembly` tracks `lastRegisteredName` and only scans if it's non-null). A `plugins/` directory
holds a plugin's own **dependency** DLLs too (QLNet, QuickFix, Ycs, Newtonsoft.Json for the three
in-tree plugins) — scanning every one of those for a same-shaped `ui-plugins/` resource would either
silently do nothing (the common case, since dependencies don't embed one) or, worse, attribute a UI
module found in a dependency to no identifiable plugin. Gating on "this assembly is a genuine plugin"
keeps the report lines meaningful: `plugin 'x' provides ui module 'y.js'` always names a real plugin.

### D3. ILRepack merge, allow-list by name, not a deny-list

Each plugin project declares only the assemblies it must carry that the host does **not** already ship
(`<PluginMergeAssembly Include="QLNet" />` etc.) — an allow-list. Everything else the plugin references
transitively (`StreamsForge.*`, `Microsoft.*`, `System.*`, `Orleans*`, `Google.Protobuf`, `Grpc.*`, and
everything pulled in through them) stays an ordinary by-name reference, resolved against the host's
already-loaded copy at runtime.

**Why an allow-list and not "merge everything except a deny-list"**: `ILRepack.Lib.MSBuild.Task`'s own
default behavior — merge every DLL sitting in the output directory — is exactly the deny-list shape,
and it is wrong for this repo: the output directory of a plugin project that references
`StreamsForge.AppCore` also contains `StreamsForge.AppCore.dll`, `StreamsForge.Contracts.dll`, and
every Orleans/gRPC/protobuf assembly pulled in transitively. Merging any of those produces a **second,
incompatible copy** of a type the host already has loaded — a merged-in `IInboundTransport` is not
interface-identical to the host's `IInboundTransport`, so a plugin built that way could not register at
all. An allow-list makes the plugin author state, per dependency, "this one is mine to carry" — the
default is "don't merge," which is also the safe default.

**Cost**: `plugins/Directory.Build.props` has to actively disable `ILRepack.Lib.MSBuild.Task`'s own
auto-target (via `$(ILRepackTargetsFile)` pointing at this repo's own targets file) rather than simply
configuring it, since the package's shipped default is the opposite of what this repo needs. QLNet's
merge alone costs 1–3.5 minutes on a clean build — paid once per real change, not per rebuild, because
the target declares `Inputs`/`Outputs` for MSBuild's normal up-to-date check.

### D4. No trimming, no Native AOT, ever — not even as an opt-in flag

`Publish.props` (both hosts) sets `PublishTrimmed=false` and `PublishAot=false` **explicitly**, not by
omission, and there is no configuration knob to turn either on.

**Why**: dynamic protobuf descriptor generation (`shared/StreamsForge.Api/Grpc/Dynamic/*` — plan 025
moved this out of the Orleans host so both flavors serve gRPC), gRPC server reflection,
SignalR's runtime type discovery, and `StreamsForgePlugins`' own `Activator.CreateInstance` +
`AssemblyLoadContext.LoadFromAssemblyPath` plugin loading are all reflection paths a trimmer or AOT
analyzer cannot see from the static call graph. Every one of them would break **silently** under
trimming — a missing member exception at runtime on a code path nobody exercised in CI, not a build
error — which is a strictly worse failure mode than "this host is 67 MB instead of 25 MB." A
trim-warning suppression list can never prove it covers every reflection path a plugin might
additionally introduce (an out-of-tree plugin's own reflection use is invisible to this repo's trimmer
analysis entirely), so the decision is permanent rather than "revisit once the warnings are clean."

### D5. Embedded content, disk-first fallback — never the reverse

The SPA (`web/dist/**`), `docs/index.html`, and the two static `.proto` files are embedded as manifest
resources at publish time (`GenerateEmbeddedFilesManifest` + `Microsoft.Extensions.FileProviders.Embedded`),
but every serving path checks the **on-disk** path first and only falls back to the embedded copy when
the disk path is absent (`EmbeddedPublishContent.TryGetProvider` in
`shared/StreamsForge.Api/StreamsForgeApiExtensions.cs`).

**Why this direction and not the reverse**: a normal `dotnet build`/`dotnet run` checkout, and every
container image built by `deploy/*/Dockerfile`, already has these on disk at a well-known path — disk
staying authoritative means single-file publish changes **nothing** about how either of those already
work. The embedded copy exists purely for the one case that previously had no answer: the publish
output copied somewhere with no repo checkout and no separately-deployed `web/dist`/`docs/` alongside
it. Reversing the priority would mean a developer's live edit to `web/dist` (during `bun run --watch`,
say) or a container's `COPY orleans/docs ./docs` would silently lose to a stale embedded copy from the
last publish — exactly backwards.

### D6. The Crdt plugin is a web plugin that replaces a disabled default; Dapr gets no equivalent

Unlike Quant and Fix (`Register()`-only), the Crdt plugin implements `IStreamsForgeWebPlugin` and uses
`ConfigureServices` to register `OrleansCrdtFacade` as `ICrdtFacade`, **replacing** the core's own
`DisabledCrdtFacade` registration. The core registers the disabled stub unconditionally so `ICrdtFacade`
always resolves to *something* — a host with no Crdt plugin gets a facade that answers every call with a
clear "not installed" error rather than a DI resolution failure (the stub moved to
`shared/StreamsForge.Api/Facades/DisabledCrdtFacade.cs` so both hosts register the same one).
`CrdtDocGrain` itself (the Orleans grain) and its Ycs dependency move into the plugin project
(Newtonsoft.Json is deliberately NOT merged: Orleans already ships it). Overruled in wave 2: the pinned
"one `AddSerializer(b => b.AddAssembly(asm))` call" was not enough — Orleans also needs the plugin's
grain classes and interfaces added to `GrainTypeOptions`, or `GetGrain<ICrdtDocGrain>` throws `Could not
find an implementation for interface`; both loops now sit in the `UseOrleans` lambda over
`StreamsForgePlugins.Loaded`. A second finding: that `GetGrain` throws at reference construction, so
`RegistryGrain.UpsertSourceAsync` guards on the plugin's presence BEFORE building the reference (every
non-crdt branch used to call `StopAsync` on it), and the readable refusal is surfaced by the sources
endpoints (400) and config import (`action: "error"`).

**Why Dapr gets nothing**: CRDT edge sync (plan 020) was already Orleans-only — the Dapr flavor stores
the `crdt` kind in its catalog but refuses to start it (plan 020 D9, `dapr/PARITY.md`). There is no
working Dapr CRDT facade to extract into a plugin; Dapr's `DisabledCrdtFacade` registration is
unconditional and permanent, not a placeholder waiting for a Dapr-side plugin that doesn't exist yet.

### D7. Built-ins load before plugins — unconditionally, no configuration to change the order

Both hosts call `DatabaseConnectors.RegisterAll()` (and any other host-hardcoded registration) **before**
`StreamsForgePlugins.LoadFrom`. A plugin — in-tree or third-party — that declares a kind name a built-in
already owns loses the name; the loader reports it as an ordinary "failed to register: … is already
registered" line, not a host-startup failure.

**Why fixed order rather than configurable priority**: a built-in kind is part of this repo's own
contract surface (documented, tested, seeded); letting a plugin silently override one would mean the
behavior of a named kind depends on which DLLs happen to be sitting in a directory, which is exactly the
non-determinism the registries' own doc comments already reject for assembly-scanning ("what runs
depends on what happens to be linked" — see `StreamsForgePlugins`' own class doc). A plugin that
genuinely needs to replace built-in behavior needs a different name, or a PR to this repo.

## Waves

| Wave | What | Status |
|---|---|---|
| 0 | Host hooks: `IStreamsForgePlugin`/`IStreamsForgeWebPlugin`, `StreamsForgePlugins.Loaded`, `StreamsForgePluginHosting`, both `Program.cs` wired, both host csprojs import an optional `Publish.props` | DONE (`0661349`) |
| 1 | Quant + Fix become single-DLL plugins (ILRepack, `PluginMergeAssembly`, `CopyBuiltInPlugins`/`PublishBuiltInPlugins`); single-file publish (`Publish.props` both hosts, `tools/publish.sh`, both Dockerfiles updated to run the native exe); embedded `ui-plugins/*.js` convention (`UiPluginsEndpoints` union + disk-wins); `StreamsForge.AppCore.Tests.PluginFixture` + `OutOfTreeKindTests`/`UiPluginsEndpointsTests` | DONE (`59abcf7`) |
| 2 | Crdt becomes the third in-tree plugin (`StreamsForge.Plugins.Crdt`, Orleans-only): `CrdtDocGrain` + `OrleansCrdtFacade` move out of the host, `ConfigureServices` replaces the shared `DisabledCrdtFacade`, merges `StreamsForge.Connectors.Crdt` + `Ycs`; two-step Orleans manifest registration; registry guard + readable refusals (see D6) | DONE — verified live with and without `plugins/`; the 5 pre-existing failures are unchanged (see below) |

## Acceptance criteria

- A plugin directory scan (`Plugins:Path`, defaulting to `plugins/` next to the binaries) that is
  missing produces zero report lines and a normally-starting host.
- A file in that directory that is not a managed assembly is reported (`could not be loaded`) and
  skipped; every other file in the directory still loads.
- A plugin whose `Register()` throws, including the registries' own duplicate-kind
  `InvalidOperationException`, is reported (`failed to register`) with the unwrapped exception message
  and skipped; every other plugin still loads.
- Deleting `plugins/StreamsForge.Plugins.Quant.dll` from a build output: the host still starts;
  `BS_PRICE` still resolves in an already-compiled, already-running pipeline; a pipeline whose SQL is
  recompiled (edit or restart) without it fails validation with `Unknown function 'BS_PRICE'`.
- Deleting `plugins/StreamsForge.Plugins.Fix.dll`: `fix`/`fix-duplex` vanish from `GET
  /api/meta/instance`'s `plugins` list; creating a new source of either kind gets a 400; an existing
  running `fix` source fails to restart with a connector-level error; the host stays up throughout.
- Deleting `plugins/StreamsForge.Plugins.Crdt.dll` (once wave 2 lands): `/api/crdt/...` returns 501; a
  `crdt` source fails to start with a readable plugin-not-installed error; the host stays up.
- A merged plugin DLL, inspected with a `PEReader`/`ildasm`: the plugin's own types are present as
  `TypeDef`s; every `StreamsForge.*` type it references resolves as an external `TypeRef` against an
  `AssemblyRef`, never as a `TypeDef` baked into the merged DLL.
- `tools/publish.sh orleans` (and `dapr`) from a clean checkout produces exactly: the native executable,
  `appsettings.json`, `plugins/` (containing the built-in plugins present at publish time), `ui-plugins/`
  (empty, with a README) — no `data/`, no `.pdb`.
- The published output, copied to a directory with **no repo checkout alongside it**: `/healthz`, `/`
  (SPA), `/docs`, login, `/api/tables`, a proto download, a plugin dropped into `plugins/` post-publish,
  and a `.js` dropped into `ui-plugins/` post-publish all work.
- `~/.dotnet/dotnet build orleans/StreamsForge.sln` and `dapr/StreamsForge.Dapr.sln` both succeed from a
  fresh clone with `git submodule update --init` run once (`external/ycs` is still required regardless
  of whether the Crdt plugin DLL is ever installed at runtime — the *project* references it to build).
- The full existing test suite passes unmodified except for the 5 pre-existing, independently verified
  failures below — this plan declares no behavior change for a host with all three in-tree plugins
  present, so any other test needing an edit is a bug in this plan's wave, not in the test.

## Cut, explicitly

- **A plugin manifest/metadata file** (version, author, declared capabilities) beyond the `Name` string
  and the assembly itself. Nothing in this plan's scope needed one; `KindVersions` already answers "what
  version is kind X" from the registered transport's own `Describe()`.
- **Plugin sandboxing or dependency isolation** (a separate `AssemblyLoadContext` per plugin). Explicitly
  rejected by D3's own reasoning — an isolated context would make a plugin's `IInboundTransport`
  interface-incompatible with the host's, breaking registration entirely.
- **Shrinking the runtime container image** from `aspnet:10.0` to `runtime-deps` now that the publish
  output is fully self-contained. Left for later — see "Found and not fixed."
- **A Dapr Crdt plugin.** CRDT edge sync stays Orleans-only; see D6.
- **Hot-reloading a plugin without a restart.** A newly created `plugins/` directory (or a new file added
  to one that didn't exist yet) needs a host restart, same as `ui-plugins/`'s own "restart to pick up a
  newly created directory" rule, which this plan did not change.

## Found and not fixed

- **`KindVersions.All()` still hardcodes `crdt` as a built-in** (`BuiltInVersion`, alongside `generator`/
  `url`/`file`/`folder`/`grpc`/`ingest` — none of which have a `TransportDescriptor` to ask, since crdt's
  driver is a grain, not a registered transport). This means `GET /api/meta/instance`'s `plugins` list
  reports `crdt` as present at a fixed version **regardless of whether the Crdt plugin DLL is actually
  installed** — a host with the plugin missing still advertises the kind, and only discovers the gap when
  something tries to use it (`/api/crdt/...` returning 501, a `crdt` source failing to start). A config
  import's `requires: [{ kind: "crdt", version: "…" }]` gate would therefore pass on a host that cannot
  actually run it. Not fixed here: making `KindVersions` ask "is a plugin providing this kind actually
  loaded" needs a per-kind→plugin-name mapping this codebase has no registry for today (a plugin's
  `Register()` call has no return value connecting the kind names it registers back to the plugin's own
  `Name`), which is bigger than this plan's scope.
- **The runtime image is still `mcr.microsoft.com/dotnet/aspnet:10.0`**, not the smaller `runtime-deps`
  base image that a genuinely self-contained single-file publish no longer needs the ASP.NET Core shared
  framework for. Not changed here — untested whether anything in either Dockerfile's runtime stage (the
  `curl` install for the Orleans healthcheck, `bash`'s `/dev/tcp` use in the Dapr entrypoint) still needs
  something `runtime-deps` drops.
- **The 6 pre-existing test failures — FIXED 2026-11-11** (root causes below the original note: five were
  Orleans' default Newtonsoft `$type` in-memory grain storage serializer refusing a plugin-defined state
  type the silo's manifest did not list — the silo log said `Type "StreamsForge.Host.Grains.CrdtDocGrainState"
  is not allowed`; replaced by `Host/Storage/GrainStorageSerializers.cs`. The sixth was a running
  connector pinned by `DelayDeactivation(Infinite)` since `bd147ce`, which `ForceActivationCollection`
  cannot collect; the test now calls `IGrainManagementExtension.DeactivateOnIdle()`.) Original note: restart/reactivation tests in `CrdtDocGrainClusterTests` (3),
  `ShardedTableClusterTests` (2) and `ConnectorGrainPolledClusterTests` (1,
  `ADeactivatedConnectorResumesFromThePersistedCursor`), failing deterministically with
  `CodecNotFoundException(Newtonsoft.Json.JsonSerializationException)` — were independently verified
  failing at `bfc421f`, **before** any of this plan's work, and re-verified unchanged after wave 2 landed
  (same 6, same signature, on a quiet machine; the sixth was only noticed in the final full run and is
  equally pre-existing at `bfc421f`). Root cause still unknown: the Newtonsoft
  `JsonSerializationException` is thrown somewhere on reactivation after
  `IManagementGrain.ForceActivationCollection`, `JsonFileGrainStorage` is System.Text.Json, and the
  silo-side inner message was never captured (the diagnosing agent was cut off). Do not describe them
  as flakes (AGENTS.md's flake list has a stated bar for entry this does not meet).
- **Node/Next.js WebSocket transport work, and anything under `server/`'s own roadmap** — out of scope
  for this plan; do not conflate `@streamsforge/server`'s own plugin-shaped extension points (`sf.source`
  handlers) with the server-plugin mechanism documented here. They share no code and no loader.

Full contributor-facing documentation: [`PLUGINS.md`](../PLUGINS.md). Operator-facing documentation:
`orleans/docs/index.html` §§ "Server plugins & out-of-tree kinds", "Console UI plugins", and "Single-file
deployment". Transport-authoring recipe (unchanged by this plan): [`TRANSPORTS.md`](../TRANSPORTS.md).
