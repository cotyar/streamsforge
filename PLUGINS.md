# Server plugins & single-file publish

How a source/sink/duplex transport, a SQL function, or a whole facade replacement gets **installed**
into a StreamsForge host instead of referenced from it — and how the three in-tree examples (Quant,
Fix, Crdt) do exactly that. Console-side UI plugins (the ES-module editor override) are a separate,
lighter mechanism; see [TRANSPORTS.md](TRANSPORTS.md)'s ["A specialized console
editor"](TRANSPORTS.md#a-specialized-console-editor-ui-plugin) — this document only points at it where
a server plugin also wants to ship one.

## The two hooks

A server plugin is one parameterless class implementing `IStreamsForgePlugin`
(`shared/StreamsForge.AppCore/Plugins/StreamsForgePlugins.cs`):

```csharp
public interface IStreamsForgePlugin
{
    string Name { get; }
    void Register();
}
```

`Register()` runs once, at host startup, after the built-in kinds have already registered and before
any source can start. That is enough for a plugin that only adds transports or SQL functions — most
plugins never need more.

A plugin that also needs the **host** — a DI service, or HTTP endpoints — additionally implements
`IStreamsForgeWebPlugin : IStreamsForgePlugin` (`shared/StreamsForge.Api/Plugins/IStreamsForgeWebPlugin.cs`):

```csharp
public interface IStreamsForgeWebPlugin : IStreamsForgePlugin
{
    void ConfigureServices(IServiceCollection services, IConfiguration configuration) { }
    void MapEndpoints(IEndpointRouteBuilder endpoints) { }
}
```

Both methods are default no-ops — implement only the one you need. `IStreamsForgeWebPlugin` lives in
`StreamsForge.Api`, not `StreamsForge.AppCore`, because its two methods take ASP.NET types and AppCore
deliberately has none (hard rule 2 in `AGENTS.md`).

**Hook order, both hosts, identical**:

1. `StreamsForgePlugins.LoadFrom(Plugins:Path)` — activates every plugin, calls `Register()` on each,
   in ordinal file-name order.
2. `StreamsForgePluginHosting.ConfigureServices(services, configuration)` — before `builder.Build()`.
3. Orleans adds every `StreamsForgePlugins.Loaded` assembly to the silo — **two registrations, both
   needed, verified live**: `siloBuilder.Services.AddSerializer(b => b.AddAssembly(asm))` (the
   serializer's type manifest: codecs, copiers, proxy invokers) AND
   `Configure<GrainTypeOptions>(o => …)` adding every concrete `IGrain` class in the assembly to
   `o.Classes` and its own grain interface(s) to `o.Interfaces`. `AddAssembly` alone still fails a
   `GetGrain<ICrdtDocGrain>` with `Could not find an implementation for interface …`, because the
   interface-to-class map is populated by the *referencing* project's generated startup code, which a
   `LoadFromAssemblyPath`'d assembly never had. Both loops run inside the `UseOrleans` lambda, which
   executes at `builder.Build()`, after `LoadFrom`. Dapr has no equivalent step (plugins cannot add
   actors).
4. `app.MapPluginEndpoints()` — after `app.MapStreamsForgeApi(...)`, so a plugin route can never shadow
   a core one.

Both host `Program.cs` files (`orleans/src/StreamsForge.Host/Program.cs`,
`dapr/src/StreamsForge.Dapr.Host/Program.cs`) drive these identically; see the comment block starting
"Out-of-tree connectors, installed rather than referenced" in each for the exact call sites.

## When to use which hook

| You are adding… | Use | Trigger |
|---|---|---|
| A SQL scalar/aggregate function | `Register()` only | The function registry (`shared/StreamsForge.Engine`'s function table) is a process-wide static — no DI, no route needed. This is the whole Quant plugin. |
| A source/sink/duplex transport kind | `Register()` only | `InboundTransports`/`PolledTransports`/`SinkTransports`/`DuplexTransports` are process-wide registries, exactly like the function table. This is the whole Fix plugin. |
| A DI service, or a replacement for a core facade the host registers **disabled by default** | `ConfigureServices` | Registering a service after the core's own registration **replaces** it for single-instance resolution (last registration wins) — the only way a plugin substitutes a "stub" the core ships (e.g. a `Disabled*Facade`) with a real implementation. Do this only when the core actually left a hook disabled for you; don't register a service nothing reads. |
| HTTP endpoints the core does not document | `MapEndpoints` | Routes should live under `/api/plugins/{name}/…` unless deliberately implementing a core-documented route (a plugin choosing to shadow a documented route is still refused nothing structurally, but it runs *after* the core map, so the core's own route always wins on a real conflict). |
| Orleans grains / custom serializers | Nothing extra — the host's two-step manifest registration (hook order step 3) picks the assembly up | You never call an Orleans API yourself — the host adds your plugin's assembly to `AddSerializer(b => b.AddAssembly(...))` for you, once it's in `StreamsForgePlugins.Loaded`. Just define the grain interface/class in the plugin assembly (or in a project the plugin project references and merges — see Crdt below) and reference it from a service you register in `ConfigureServices`. |
| A console editor for your kind | Embedded `ui-plugins/*.js` (or a loose file) | Only when the generic descriptor-driven form genuinely can't express the kind (a topic browser, a connection tester). Not a server-plugin hook at all — see [TRANSPORTS.md](TRANSPORTS.md#a-specialized-console-editor-ui-plugin). A plugin DLL can carry its module as an embedded resource so one file does both jobs — see "Embedded UI modules" below. |
| A config dimension for your kind (a new field, a new option) | The open `settings` bag, never a typed field | `ConnectorConfig.Settings`/`SinkSpec.Settings` is a string dictionary the platform already stores, exports and imports. Your transport's `Describe()` declares which keys exist and which are secret; `SecretWalk` cannot see into a dictionary, so masking is driven by your descriptor, not attributes. The one thing the bag cannot express is a nested optional group (see `TRANSPORTS.md`). |

**Do NOT:**

- Put ASP.NET types in `StreamsForge.AppCore` — that's what `IStreamsForgeWebPlugin` living in
  `StreamsForge.Api` is for (hard rule 2).
- Merge `StreamsForge.*`, `Microsoft.*`, `System.*`, `Orleans*`, `Google.Protobuf` or `Grpc.*` into your
  plugin DLL — the host already ships all of them; a merged-in duplicate produces two incompatible
  copies of e.g. `IInboundTransport` (see "Build & packaging" below).
- Try to shadow a built-in kind name — plugins load *after* built-ins register (`DatabaseConnectors
  .RegisterAll()` then `LoadFrom`), so a plugin claiming an existing name loses the name and the loader
  reports it as a normal registration failure, not a host-startup failure.
- Rely on load order **between plugins** beyond file-name ordinal (`Directory.EnumerateFiles(dir,
  "*.dll").Order(StringComparer.Ordinal)`). Two plugins that must see each other's registrations in a
  specific order have no contract for that beyond renaming the files — don't build on it.

## For which implementations — the three in-tree plugins

All three live under `plugins/` at the repo root, each its own project, none referenced by either host
csproj as a normal `ProjectReference` (both use `ReferenceOutputAssembly="false"` — build-order only,
nothing links into the host binary).

### Quant — `Register()` only, SQL functions

`plugins/StreamsForge.Plugins.Quant/QuantPlugin.cs`:

```csharp
public sealed class QuantPlugin : IStreamsForgePlugin
{
    public string Name => "quant";
    public void Register() => StreamsForge.Quant.QuantFunctions.RegisterAll();
}
```

One line. `StreamsForge.Quant` (the QLNet-backed pricing scalars — `BS_PRICE`, `BS_DELTA`, `BS_GAMMA`,
`BS_VEGA`, `BS_THETA`, `BOND_PRICE`, `BOND_DV01`, `BOND_DURATION`, `IRS_NPV`, `IRS_DV01`, `FX_FWD`) and
`QLNet` itself are merged into the plugin DLL (`PluginMergeAssembly` entries for both) — the host no
longer references `StreamsForge.Quant` at all once this plugin replaces the old direct reference. No
`ConfigureServices`, no `MapEndpoints`, no UI module: a scalar function needs none of them.

### Fix — `Register()` only, transports

`plugins/StreamsForge.Plugins.Fix/FixPlugin.cs`:

```csharp
public sealed class FixPlugin : IStreamsForgePlugin
{
    public string Name => "fix";
    public void Register() => StreamsForge.Connectors.Fix.FixConnectors.RegisterAll();
}
```

Same one-line shape as Quant, registering the `fix` (receive-only session source) and `fix-duplex`
(bidirectional session) transport kinds instead of functions. `StreamsForge.Connectors.Fix` and
`QuickFix` (QuickFIXn.Core's actual assembly name — not `QuickFIXn`) are merged in. See
[`TRANSPORTS.md`](TRANSPORTS.md)'s FIX sections for the wire-level detail; this document only covers
how the kind gets *installed*.

### Crdt — `ConfigureServices` + grains + a merged native dependency

`plugins/StreamsForge.Plugins.Crdt` (Orleans-only — Dapr has no equivalent, see below) is the plugin
that needs the second tier: `CrdtPlugin : IStreamsForgeWebPlugin`, `Name` `"crdt"`.
`ConfigureServices` registers `OrleansCrdtFacade` as `ICrdtFacade`, **replacing** the host's own default
registration of `DisabledCrdtFacade` — the pattern the decision table above calls out as the only
reason to touch `ConfigureServices`: the core ships a disabled stub on purpose so a host with no Crdt
plugin installed still resolves `ICrdtFacade` to *something*, just one that answers every call with "not
installed" rather than throwing a missing-DI-registration exception.

The plugin project also carries `CrdtDocGrain` (the Orleans grain that owns one Yjs document and
projects it to rows) and merges `StreamsForge.Connectors.Crdt` and `Ycs` (the `external/ycs` submodule's
build output) into its output DLL. Ycs brings no third-party dependency of its own: its v1-format JSON
(formatting attributes, embeds) goes through System.Text.Json with `JSON.stringify` number and string
rules (`external/ycs/src/Ycs/Utils/YJson.cs`), not Newtonsoft.Json as it did before 2026-11-11. The grain type is
discoverable by the silo through the host's two-step manifest registration (hook order step 3 above),
with zero CRDT-specific code in the host.

**What a grain must satisfy to be picked up by that registration** (the host reflects over the plugin
assembly; nothing is declared explicitly): (1) it is an ordinary concrete Orleans grain — a
non-abstract class deriving from `Grain` and implementing its own interface, which extends an
`IGrainWith*Key`; the host's filter is literally `type.IsClass && !type.IsAbstract &&
typeof(IGrain).IsAssignableFrom(type)`. (2) Its grain interface lives outside the `Orleans` namespace
(in `StreamsForge.Abstractions`, like `ICrdtDocGrain`, or in the plugin itself when only the plugin's
own code calls it) — every non-`Orleans*` grain interface on the class is added to
`GrainTypeOptions.Interfaces`. (3) It is declared in the plugin's **primary** assembly, never in a
merged dependency: Orleans codegen emits the assembly's `TypeManifestProvider` and the grain's invokers
into the assembly that declares the grain, and ILRepack keeps assembly-level attributes only from the
primary, so a grain inside a merged DLL loses its manifest and `AddAssembly` registers nothing for it
(Crdt: the grain is in the plugin project; `StreamsForge.Connectors.Crdt`, which is merged, carries no
grains). (4) The project has Orleans codegen — `Microsoft.Orleans.Sdk`, transitively via the
`StreamsForge.Abstractions` reference — and its state types carry `[GenerateSerializer]`/`[Id(n)]`.
What a grain cannot do: configure the silo. There is no plugin hook inside `UseOrleans`, so a plugin
grain uses only the providers the host already registers — `[PersistentState("…",
StreamConstants.StorageName)]` (the `JsonFileGrainStorage` under `DataDir`) and
`GetStreamProvider(StreamConstants.ProviderName)` — and cannot declare a storage or stream provider of
its own. And nothing calls a plugin grain unless the plugin does: Crdt is reached from `RegistryGrain`
because the `crdt` kind is dispatched by the core; an out-of-tree grain is reached from the plugin's own
`MapEndpoints` routes (resolve `IGrainFactory` from the request's services) or from its transport.

`ICrdtDocGrain` (the grain interface, in `StreamsForge.Abstractions`), `ICrdtFacade` (in
`StreamsForge.Contracts`), `CrdtEndpoints` (the `/api/crdt/...` routes) and
`SourceSchemaService.ValidateCrdt` all stay in the core — only the Orleans-specific implementation
(the grain class and the facade that talks to it) moved into the plugin. Without the plugin installed:
`GET /api/sources/{name}/crdt` (and the other CRDT routes) answer `501 {"error":"this build has no CRDT
document runtime"}` from `DisabledCrdtFacade` (now shared: `shared/StreamsForge.Api/Facades/`), and
creating or starting a `crdt` source is refused by `RegistryGrain`'s guard with `source 'x' is kind
'crdt', but no 'crdt' plugin is loaded on this host — install plugins/StreamsForge.Plugins.Crdt.dll (or
check the Plugins:Path this instance was started with) before starting a CRDT source.` — surfaced as
HTTP 400 on `POST`/`PUT /api/sources` and as an `action: "error"` entry (HTTP 200, `ok: false`) in
`POST /api/config/import`. The guard sits in the registry rather than the grain because
`GetGrain<ICrdtDocGrain>` throws at reference-construction time when no implementation is registered,
which would otherwise break *every* source upsert (the non-crdt branches call `StopAsync` on that
reference for the kind-switched-away cleanup). The host stays up either way. Test projects that boot a
`TestCluster` (`StreamsForge.Host.Tests`, `StreamsForge.Engine.Tests`) reference the plugin project
directly so the grain exists in their silo — that path bypasses `LoadFrom` and ILRepack by design; only
a live host proves the merged DLL. The Dapr flavor never
gets a Crdt plugin at all; it keeps using `DisabledCrdtFacade` directly, unconditionally — CRDT edge
sync (plan 020) was always an Orleans-only feature (`orleans/docs/index.html`'s CRDT section; Dapr's own
gap list in `dapr/PARITY.md`), so there is no "Dapr equivalent" to build. `external/ycs` remains a
required submodule regardless of whether the plugin DLL is ever installed, because the *plugin project*
references it to build — `git submodule update --init` is still needed for a fresh clone to compile
either solution (AGENTS.md's build/test/run section).

### An out-of-tree template

A plugin outside this repo entirely needs, at minimum:

```xml
<!-- YourCompany.Orion.csproj -->
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <!-- A class library does not copy its PackageReference DLLs to its own output the way an
         executable does — without this, the plugin's type still registers fine, but the first real
         operation (probe, connect, subscribe) fails with a FileNotFoundException for the first
         package DLL it needs. Copy the whole output directory into plugins/, not just the plugin DLL. -->
    <CopyLocalLockFileAssemblies>true</CopyLocalLockFileAssemblies>
  </PropertyGroup>
  <ItemGroup>
    <!-- Reference the host's public surface by package or project — however your build reaches it.
         Never merge it (see "Build & packaging" below): the host already ships it. -->
    <PackageReference Include="StreamsForge.AppCore" Version="…" />
  </ItemGroup>
  <ItemGroup>
    <!-- Optional: a console UI module (.js/.mjs/.ts/.tsx) travels inside the same DLL. -->
    <EmbeddedResource Include="ui-plugins/*" LogicalName="ui-plugins/%(Filename)%(Extension)" />
  </ItemGroup>
</Project>
```

```csharp
// OrionPlugin.cs
public sealed class OrionPlugin : IStreamsForgePlugin
{
    public string Name => "Orion connector 1.2.0";
    public void Register() => InboundTransports.Register(new OrionTransport());
}
```

```js
// ui-plugins/orion.js — only if the generic config form can't express Orion's config.
const { react, registerTransportEditor } = window.streamsforge
registerTransportEditor('orion', OrionEditor)
```

Build with plain `dotnet build` (no ILRepack needed unless you bring a third-party dependency the host
does not ship — see the next section for when you do), then copy `bin/<config>/net10.0/OrionPlugin.dll`
(or wherever your build put it) into the host's `plugins/` directory. That's the whole install. See
[`TRANSPORTS.md`](TRANSPORTS.md#an-out-of-tree-kind-install-dont-fork) for the transport interface
itself, the config-contract rules, and the `settings`-bag convention.

## Build & packaging

An in-tree plugin under `plugins/` is **ILRepack-merged** into one self-contained DLL after every
build. This is what makes the operator-facing install story "copy one file" instead of "copy this DLL
and its four dependencies in the right relative layout."

**The merge rule**: merge only what the host does **not** already ship. `StreamsForge.*`, `Microsoft.*`,
`System.*`, `Orleans*`, `Google.Protobuf` and `Grpc.*` — and everything pulled in transitively through
them (NATS, YamlDotNet, Cronos, …) — are referenced **by name**, never merged; a plugin project
declares only its *own* extra dependencies (QLNet, QuickFix, Ycs) as
`<PluginMergeAssembly Include="…" />` items, by assembly file base name with no extension.

**Why**: plugins load into the host's own `AssemblyLoadContext.Default` so they share its
`StreamsForge.AppCore`/`StreamsForge.Contracts` types — a transport implementing a *merged-in copy* of
`IInboundTransport` would implement a **different type** from the host's and could not register at all.
On any other dependency-version conflict, the host's already-loaded copy wins (default ALC semantics),
so merging a duplicate of something the host ships is pure waste at best and a silent version mismatch
at worst.

**Where the rule lives**: `plugins/Directory.Build.props` (net10.0, `CopyLocalLockFileAssemblies=true`
so ILRepack has an actual file on disk to merge — a class library does not normally copy a
`PackageReference`'s DLL to its own output the way an executable does) and
`plugins/ILRepack.Plugins.targets` (the actual `MergePluginAssembly` target, `AfterTargets="Build"`,
using `ILRepack.Lib.MSBuild.Task` 2.0.46). `Directory.Build.props` also points
`$(ILRepackTargetsFile)` at the shared targets file — that property is how it disables
`ILRepack.Lib.MSBuild.Task`'s own default auto-merge target (which merges *everything* in the output
directory and only on Release builds; neither is right here).

**Incremental, not free**: the target declares `Inputs`/`Outputs` (the plugin's own DLL plus every
`PluginMergeAssembly`, vs. `$(OutDir)merged\$(TargetName)$(TargetExt)`), so MSBuild's normal
up-to-date check skips it when nothing changed. QLNet alone (Quant's dependency) takes ILRepack roughly
1–3.5 minutes to merge the first time — it is a large, near-verbatim port of QuantLib — so budget for
that on a clean build or after touching `StreamsForge.Quant`. Runs on **every** `Build`, Debug included
(not gated to Release), because `dotnet run`/F5 needs a merged plugin too for the dev inner loop to have
the quant functions and fix kinds without a publish step.

**Verifying a merged DLL** (the PEReader check): open `bin/<config>/net10.0/merged/<Plugin>.dll` in
`ildasm`, `dotnet-ildasm`, or a `System.Reflection.Metadata.PEReader`/`MetadataReader` and confirm two
things — (1) the plugin's own private types (e.g. `QuantPlugin`, the Quant scalar function classes) are
present in the merged assembly's type table, and (2) `StreamsForge.*` type references resolve as
**external** `TypeRef`s (an `AssemblyRef` row pointing at `StreamsForge.AppCore` etc.), never as
`TypeDef`s baked into this DLL. The second check is the one that actually proves the merge rule was
followed — a `StreamsForge.AppCore.Plugins.IStreamsForgePlugin` `TypeDef` inside the merged DLL would
mean it got merged by mistake and the plugin loads a second, incompatible copy.

**The `$(OutDir)plugins` / `$(PublishDir)plugins` contract**: each host csproj (not the plugin project)
owns two MSBuild targets — `CopyBuiltInPlugins` (`AfterTargets="Build"`) copies each built-in plugin's
merged DLL from its own `bin/$(Configuration)/net10.0/merged/*.dll` into `$(OutDir)plugins`, and
`PublishBuiltInPlugins` (`AfterTargets="Publish"`, `DependsOnTargets="CopyBuiltInPlugins"`) copies the
same files into `$(PublishDir)plugins`. This is why `dotnet run` and `dotnet publish` both have the
built-in plugins with zero manual steps, and why `tools/publish.sh` doesn't need its own copy logic for
them (it still carries a *build output* `plugins/` directory into the publish output as a fallback, for
the rare case a copy target landed the DLLs somewhere the publish step's own output tree didn't see —
see that script's step 4 comment).

**Adding a fourth built-in plugin to both hosts** needs exactly four edits, mirroring what Quant/Fix/Crdt
already have:

1. In the host csproj's existing `<ItemGroup>` of plugin references, add
   `<ProjectReference Include="..\..\..\plugins\StreamsForge.Plugins.YourPlugin\StreamsForge.Plugins.YourPlugin.csproj" ReferenceOutputAssembly="false" />`.
2. In `CopyBuiltInPlugins`'s `<_BuiltInPluginDlls>` item group, add
   `<_BuiltInPluginDlls Include="..\..\..\plugins\StreamsForge.Plugins.YourPlugin\bin\$(Configuration)\net10.0\merged\StreamsForge.Plugins.YourPlugin.dll" />`.
3. Do both of the above in **both** `orleans/src/StreamsForge.Host/StreamsForge.Host.csproj` and
   `dapr/src/StreamsForge.Dapr.Host/StreamsForge.Dapr.Host.csproj` — unless the plugin is genuinely
   Orleans-only (like Crdt), in which case it only goes in the Orleans host's csproj.
4. Nothing else — `PublishBuiltInPlugins` already `DependsOnTargets="CopyBuiltInPlugins"`, so step 2
   alone covers both `dotnet build` and `dotnet publish`.

## Runtime contract

**Load order**: built-in registrations first (`DatabaseConnectors.RegisterAll()` and any other
host-hardcoded `RegisterAll()` calls), **then** `StreamsForgePlugins.LoadFrom(Plugins:Path)` — so a
plugin can never shadow a built-in kind name, only lose to it. Within the plugin directory, files load
in `StringComparer.Ordinal` file-name order; within one assembly, plugin types activate in
`StringComparer.Ordinal` full-type-name order. `LoadFrom` itself is **two-pass**: every DLL in the
directory is loaded first, then each loaded assembly is scanned for `IStreamsForgePlugin` types —
order-independent, so a dependency that sorts after its plugin by filename no longer produces a
spurious "could not be loaded" line. Once a plugin registers, its referenced assembly versions are
compared against what the host actually has loaded, and a plugin built against a **newer** version than
the host logs `plugin 'X' references A 8.0.0.0 but the host has 6.0.0.0 loaded — the host copy wins; a
TypeInitializationException at first use means this` (silent when the host's copy is the same or newer).

**Where it looks**: `Plugins:Path` configuration key, defaulting to `plugins/` next to
`AppContext.BaseDirectory` (the exe's own directory — including under single-file publish, where
`AppContext.BaseDirectory` still resolves correctly). A missing directory is silent, not an error — the
overwhelmingly common case is no plugins installed at all.

**Report lines**, one per outcome, written to stdout by both hosts (`Console.WriteLine($"[plugins]
{line}")`):

```
[plugins] plugin 'quant' (StreamsForge.Plugins.Quant.QuantPlugin) registered
[plugins] plugin 'fix' (StreamsForge.Plugins.Fix.FixPlugin) registered
[plugins] plugin 'crdt' (StreamsForge.Plugins.Crdt.CrdtPlugin) registered
[plugins] plugin 'test-kind-plugin' provides ui module 'test-kind.js'
[plugins] plugin 'YourCompany.Orion.OrionPlugin' failed to register: an inbound transport for kind 'orion' is already registered
[plugins] plugin assembly 'broken.dll' could not be loaded: Could not load file or assembly …
```

**Failure modes, all non-fatal to host startup**:

- A file that is not a managed assembly (bad PE image, wrong architecture) — reported as "could not be
  loaded", the file skipped, everything else in the directory still loads.
- A plugin type whose constructor throws, or whose `Register()` throws (including the registries' own
  duplicate-kind `InvalidOperationException`) — reported as "failed to register" with the unwrapped
  inner exception message, that plugin skipped, everything else still loads.
- A directory that doesn't exist — silent, zero report lines, host starts normally with no plugins.

None of these can keep the host from starting; a broken plugin costs you that plugin's functionality,
never the whole process.

**Default `AssemblyLoadContext`, deliberately**: plugins load into `AssemblyLoadContext.Default`, the
same context the host itself runs in — not an isolated context. This is what lets a plugin's
`IInboundTransport` implementation be recognized as the *same* `IInboundTransport` type the host's
registries expect (see "Build & packaging" above for why isolation would break this). The cost is the
usual one for shared-context plugins: a plugin's dependency versions are not isolated from the host's,
and on any conflict the host's already-loaded copy wins — which is exactly why the merge rule above
exists (don't ship a copy of something that will lose anyway).

**What a missing plugin does to existing catalog entries**: nothing crashes. Quant scalars used by an
already-compiled, already-running pipeline keep working until that pipeline is edited or the host
restarts — SQL compiled once does not re-resolve function names later. A pipeline whose SQL is
recompiled (edit, or restart) without the Quant plugin present fails validation with `Unknown function
'BS_PRICE'`. `fix`/`fix-duplex` kinds vanish entirely from `GET /api/meta/instance`'s `plugins` list;
creating a new source of that kind gets a 400 (`kind 'fix' is not recognized (expected one of: …)`); an
existing `fix` source that was already `Running` fails to (re)start with a connector-level error, but
the host itself stays up. A `crdt` source behaves the same way once its plugin is the only thing
carrying the working facade — see the Crdt section above for `DisabledCrdtFacade`'s specific error
shape. No seed data and no test in this repo depends on any of the three being present, by design —
losing one is a degraded instance, never a broken one.

## Testing a plugin

**Unit-test the plugin class directly** — `Register()` and (if implemented) `ConfigureServices`/
`MapEndpoints` are plain methods on a plain class; call them against fakes/an in-memory
`IServiceCollection` the way any other unit under test would be. This does not require the loader at
all.

**The `PluginFixture` pattern** (`shared/StreamsForge.AppCore.Tests.PluginFixture/`) is for testing the
**loader** itself — `StreamsForgePlugins.LoadFrom` and `UiPluginsEndpoints`' embedded-module path both
need a *real assembly on disk* to load, since the loader reads files, not in-memory types. The fixture
project:

- Is a standalone class library, not part of either `.sln`, referenced by
  `shared/StreamsForge.AppCore.Tests` with `ReferenceOutputAssembly="false"` (build-order only — the
  test project must never load this assembly the normal way, because the test then also copies the
  *built DLL* into a temp directory and loads it via `StreamsForgePlugins.LoadFrom`; loading the same
  assembly identity twice into one `AssemblyLoadContext` collides).
- Contains `TestKindPlugin` (an `IStreamsForgePlugin` that registers nothing — it exists purely to
  prove the embedded UI-module convention) and one embedded `ui-plugins/test-kind.js`.
- Is located at test time by searching `../../../../StreamsForge.AppCore.Tests.PluginFixture/bin/**`
  for `StreamsForge.AppCore.Tests.PluginFixture.dll`, filtered to the `bin` tree only — `obj/**/ref` and
  `obj/**/refint` hold reference-assembly copies with the same simple name but no method bodies, and
  loading one of those throws "Reference assemblies cannot be loaded for execution."

`OutOfTreeKindTests` (loader-level: report lines, embedded resource discovery, resource content) and
`UiPluginsEndpointsTests` (HTTP-level: `GET /api/ui-plugins` listing, disk-wins-over-embedded
precedence) both build on this fixture — read those two files for the exact assertions before writing
a new plugin-loader test, so a new one doesn't duplicate coverage that already exists.

**Live check recipe** — verify an actual plugin DLL against a running host on isolated ports (never the
dev server on 5199/5299 or 5399/5499 — see AGENTS.md's ports section):

```bash
mkdir -p /tmp/sf-plugin-check/plugins
cp bin/Release/net10.0/merged/YourPlugin.dll /tmp/sf-plugin-check/plugins/
~/.dotnet/dotnet run --project orleans/src/StreamsForge.Host -- \
  --Http:Port 6801 --Grpc:Port 6802 --DataDir /tmp/sf-plugin-check/data \
  --Plugins:Path /tmp/sf-plugin-check/plugins
# watch stdout for "[plugins] plugin '...' registered"
curl -s http://localhost:6801/api/meta/instance | jq .plugins   # your kind should be listed
# ... exercise the kind (create a source/pipeline using it, or run a SQL function) ...
# kill the instance and rm -rf /tmp/sf-plugin-check when done
```

## See also

- [`TRANSPORTS.md`](TRANSPORTS.md) — the transport interfaces themselves (`IInboundTransport`,
  `IPolledTransport`, `ISinkTransport`, `IDuplexTransport`), the config-contract rules, the `settings`
  bag in full, and the console UI plugin contract (props, registry, a worked example module). This
  document only covers how a plugin *installs itself*; that one covers what it installs.
- `orleans/docs/index.html` §§ "Server plugins & out-of-tree kinds" and "Console UI plugins" — the
  operator-facing version of this document (curl recipes, what an operator sees, no C#).
- `plans/022-plugins-and-single-file.md` — the plan this shipped under: decisions, per-wave outcomes,
  and the found-and-not-fixed list.
