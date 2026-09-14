using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans;
using Orleans.Hosting;
using Orleans.Serialization;
using StreamsForge.Abstractions;
using StreamsForge.Api;
using StreamsForge.Api.Plugins;
using StreamsForge.AppCore.Discovery;
using StreamsForge.AppCore.Environments;
using StreamsForge.AppCore.Net;
using StreamsForge.Connectors.Database;
using StreamsForge.Host.Facades;
using StreamsForge.Host.Grpc;
using StreamsForge.Host.Services;
using StreamsForge.Host.Storage;
using StreamsForge.Host.Streaming;

var builder = WebApplication.CreateBuilder(args);

// Co-hosted process listens on http://localhost:5199 (REST/SignalR/SPA, HTTP/1.1) and
// http://localhost:5299 (gRPC, cleartext h2c — HTTP/2-only, no ALPN without TLS) by default;
// ASPNETCORE_URLS (if set) wins and takes the single-port path below instead of these two explicit
// Kestrel endpoints.
//
// TLS, in each of the two branches:
//
// - Two-listener branch (no --urls): set `Tls:Enabled=true` and both listeners become https, using
//   Kestrel's own default certificate section — `Kestrel:Certificates:Default:{Path,KeyPath}` for a
//   PEM pair, `{Path,Password}` for a PFX, or `{Subject,Store}` for one out of the OS store. The gRPC
//   listener stays HTTP/2-only; with TLS it is no longer h2c but ALPN-negotiated h2, which every gRPC
//   client speaks without a special switch. `tools/tls/dev-cert.sh <dir>` mints a development pair and
//   prints the two arguments. Startup FAILS FAST when Tls:Enabled is set with no certificate
//   configured — a "TLS on" flag that silently served cleartext is the worst outcome available here.
//
// - Single-port branch (--urls): `--urls https://…` plus the same certificate section, and no
//   Tls:Enabled needed — the scheme in the URL is what turns TLS on there, and Kestrel resolves the
//   certificate from that section itself. This is also the branch where Http1AndHttp2 finally does
//   what the wishlist wanted (see the long note in that branch): real ALPN multiplexing of REST and
//   gRPC on one port requires TLS, and this is how it gets it.
//
// `Tls:Enabled` also turns on HSTS in the shared API pipeline (see StreamsForgeApiExtensions).
// OUTBOUND trust — what this host accepts when IT dials a peer/sink/url source — is a separate axis
// configured just below via OutboundTls.
//
// PORT (the PaaS convention: Cloud Run, Heroku, fly.io all set it) moves the HTTP port, and the gRPC
// port follows at PORT+100 — the same +100 relationship the two defaults already have. Without this,
// `PORT=6199 dotnet run` silently still bound 5199/5299, which on a developer machine means landing on
// whatever else already owns those ports. Http:Port / Grpc:Port still win where they are set, so an
// explicit pair can always split the two apart.
var envPort = builder.Configuration.GetValue<int?>("PORT");
var httpPort = builder.Configuration.GetValue("Http:Port", envPort ?? 5199);
// Resolved out here, not inside the `if`, because StreamsForgeApiOptions below reports this same number
// to clients — computing it twice is how the reported port and the bound port drift apart.
var grpcPort = builder.Configuration.GetValue("Grpc:Port", envPort is { } p ? p + 100 : 5299);

// Outbound TLS trust, configured ONCE for every client this process dials with (federated gRPC
// subscriber, HTTP sink, peer probe, url source, OpenAPI derive). Must run before anything can dial:
// each of those call sites captures its handler in a static Lazy the first time it is used, and
// OutboundTls.Configure deliberately throws rather than silently apply to only some of them.
//
// No logger here — the DI logging pipeline does not exist until builder.Build(). The one thing worth
// warning about (AcceptAnyCertificate) is re-emitted through app.Logger after the build, so it lands
// in the real log rather than only on stdout.
OutboundTls.Configure(
    builder.Configuration[OutboundTls.TrustedCaPathKey],
    builder.Configuration.GetValue(OutboundTls.AcceptAnyCertificateKey, false));

var tlsEnabled = builder.Configuration.GetValue("Tls:Enabled", false);

if (string.IsNullOrEmpty(builder.Configuration["urls"]))
{
    // Explicit Http:Port/Grpc:Port split (or the bare defaults above) — two listeners, one protocol
    // each, byte-for-byte the pre-#19 shape. This is now an OVERRIDE for anyone who genuinely wants
    // REST/SignalR and gRPC apart (a deployment that has two ports to spend, or a TLS-terminating
    // front end that only forwards one protocol per port) rather than the only option.
    //
    // IPAddress.Any (all IPv4 interfaces), not ListenLocalhost and NOT ListenAnyIP:
    //
    // - Not ListenLocalhost, for the original reason: a loopback-only listener is unreachable from
    //   outside the process's own network namespace, which is exactly what made a published Docker port
    //   a dead end for gRPC even before the single-port work below existed (see the Dockerfile's note).
    //   IPAddress.Any is every IPv4 interface, not loopback, so Docker port publishing is unaffected.
    //
    // - Not ListenAnyIP, which was the shape here until 2026-08-21, because it binds the IPv6 wildcard
    //   `[::]` in DUAL-STACK mode and this platform crashes on that. Measured, not theorised: driving the
    //   TypeScript client's contract suite over `localhost` (which resolves to BOTH ::1 and 127.0.0.1, so
    //   the client flips address families) makes Kestrel's socket accept loop throw
    //   `System.ArgumentException: The supplied System.Net.SocketAddress is an invalid size for the
    //   System.Net.IPEndPoint end point` out of `SocketConnectionListener.AcceptAsync`. That exception is
    //   UNHANDLED inside the accept loop, so it does not fail one connection — it kills the listener
    //   ("The connection listener failed to accept any new connections") and the host then shuts down.
    //   Every later request gets ECONNREFUSED. Reproduced 4/4 on macOS 26.5.1 / .NET SDK 10.0.302, at
    //   both high and low machine load, and identically at a pre-plan-020 commit, so it is neither a
    //   load flake nor anything the CRDT work introduced. Switching these two lines to IPAddress.Any
    //   takes that suite from 6/14 to 14/14 with zero listener crashes.
    //
    // The cost, stated: this host no longer answers on IPv6. That is a real regression for an
    // IPv6-only client and is the accepted price of not having the whole listener die. The underlying
    // defect is in the runtime's dual-stack accept path, not here — if a fixed .NET revision lands,
    // this can go back to ListenAnyIP and the contract suite is the thing that will tell you whether it
    // is safe to (see AGENTS.md's verification gates, which now list it for exactly that reason).
    //
    // Tls:Enabled adds UseHttps() to BOTH listeners. The no-argument overload is deliberate: it takes
    // whatever Kestrel's own configuration loader already parsed out of Kestrel:Certificates:Default,
    // so PEM pairs, PFX+password and OS-store lookups are all supported here without this file knowing
    // any of those shapes. The gRPC listener keeps Http2 — under TLS that is ALPN-negotiated h2 rather
    // than prior-knowledge h2c, which is what a gRPC client dialling https:// expects anyway.
    if (tlsEnabled
        && string.IsNullOrWhiteSpace(builder.Configuration["Kestrel:Certificates:Default:Path"])
        && string.IsNullOrWhiteSpace(builder.Configuration["Kestrel:Certificates:Default:Subject"]))
    {
        // Fail fast, loudly. The alternative — booting cleartext with Tls:Enabled=true in the config —
        // is a security misconfiguration that looks exactly like a working server.
        throw new InvalidOperationException(
            "Tls:Enabled is true but no server certificate is configured. Set either "
          + "Kestrel:Certificates:Default:Path (+ :KeyPath for a PEM pair, or + :Password for a PFX) "
          + "or Kestrel:Certificates:Default:Subject (+ :Store) so Kestrel has a certificate to serve. "
          + "For a development pair: tools/tls/dev-cert.sh <out-dir> [host-or-ip ...], which prints the "
          + "exact arguments to pass.");
    }

    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.Listen(IPAddress.Any, httpPort, o =>
        {
            o.Protocols = HttpProtocols.Http1;
            if (tlsEnabled)
            {
                o.UseHttps();
            }
        });
        kestrel.Listen(IPAddress.Any, grpcPort, o =>
        {
            o.Protocols = HttpProtocols.Http2;
            if (tlsEnabled)
            {
                o.UseHttps();
            }
        });
    });
}
else
{
    // Wishlist #19, as literally specified: gRPC as a second PROTOCOL on the one port --urls/PORT
    // already picks, via HttpProtocols.Http1AndHttp2 on that single endpoint.
    //
    // CORRECTION TO THE WISHLIST'S OWN ANALYSIS, MEASURED AGAINST A REAL RUNNING HOST — read before
    // touching this branch. The wishlist assumed "Kestrel detects the HTTP/2 preface on a cleartext
    // listener, so REST, SignalR and gRPC coexist on one port with no TLS/ALPN". That is not how
    // stock Kestrel behaves. Booting this exact binary with --urls http://localhost:9199 and setting
    // Http1AndHttp2 (whether via ConfigureEndpointDefaults or an explicit Listen*/ListenAnyIP call —
    // both were tried) makes Kestrel itself log "HTTP/2 is not enabled for ... TLS is not enabled ...
    // Connections to this endpoint will use HTTP/1.1", and a real h2c "prior knowledge" gRPC call
    // against that port times out. This isn't a misconfiguration on this file's part: automatic
    // protocol selection on a CLEARTEXT Kestrel endpoint is an open, unshipped ASP.NET Core feature
    // request (dotnet/aspnetcore#56984, filed July 2024, still open) — Microsoft's own words on
    // current behavior are "If an endpoint is cleartext (doesn't have TLS) then the connection always
    // falls back to HTTP/1.1. If a client sends a prior knowledge H2C request to the server, the
    // server will error with HTTP_1_1_REQUIRED." Genuine HTTP/1.1-and-HTTP/2 multiplexing on ONE
    // Kestrel endpoint requires TLS + ALPN, full stop, in every version of Kestrel available as of
    // this change.
    //
    // WHY THE SETTING STAYS ANYWAY. Three reasons: (1) it is exactly what the wishlist's own "Do" text
    // asks for, verbatim; (2) it is not a regression — REST/SignalR on this endpoint are completely
    // unaffected (proven below), so the only thing NOT achieved is the gRPC half, which was ALSO not
    // achieved before this change (an --urls deploy never opened a gRPC listener at all); (3) it is
    // the forward-compatible choice — the day this endpoint gains a certificate (Kestrel:Certificates:
    // Default in config, no code change needed here), Http1AndHttp2 starts doing real ALPN-negotiated
    // multiplexing for free. Cloud Run's own HTTP/2 end-to-end mode (`--use-http2` / a port named
    // "h2c") is the other path to a working single port in production: Cloud Run's edge terminates the
    // client's real TLS and then forwards EVERY request to the container as HTTP/2 cleartext, so the
    // container never has to disambiguate REST-vs-gRPC on its own cleartext listener at all — Cloud
    // Run's edge already did that. Neither path is exercised by this task's local verification (no
    // cert, no Cloud Run instance to hand), so what's PROVEN here is REST/SignalR unaffected and gRPC
    // still gated on one of the two paths above; what's NOT proven is either path actually closing the
    // gap end-to-end, and that should not be assumed without testing it for real.
    builder.WebHost.ConfigureKestrel(kestrel =>
        kestrel.ConfigureEndpointDefaults(o => o.Protocols = HttpProtocols.Http1AndHttp2));
}

// Streams:Transport selects the stream transport. "pull" (DEFAULT) is Orleans' stock memory-stream
// path, untouched. "push" swaps in StreamsForge.Host.Streaming's in-process push bus under the SAME
// provider name — every producer/consumer call site is identical in both modes (see PushStreamBus and
// PushStreamProvider's class docs for the ordering/deadlock/backpressure reasoning).
var streamTransport = (builder.Configuration["Streams:Transport"] ?? "pull").Trim().ToLowerInvariant();
if (streamTransport is not ("pull" or "push"))
{
    throw new InvalidOperationException(
        $"Unknown Streams:Transport '{streamTransport}'. Valid values: 'pull' (default, Orleans memory streams) or 'push'.");
}

builder.Host.UseOrleans(siloBuilder =>
{
    // Silo/gateway ports are configurable so two hosts can run side by side on
    // one machine — e.g. the OTC demo and the aqua-demo fish-farm demo, which
    // must not share an engine (a materialized table's state is persisted by
    // JsonFileGrainStorage under DataDir and survives dropping the table, its
    // source, and re-provisioning, so one engine silently mixes both demos).
    //
    // The parameterless overload hardcodes 11111/30000, and a second host then
    // collides on them: the symptom is not a clean bind error but Kestrel's
    // listener dying later with "The connection listener failed to accept any
    // new connections / SocketAddress is an invalid size for the IPEndPoint",
    // taking the HTTP API down with it. Defaults below are Orleans' own, so a
    // host started without these flags behaves exactly as before.
    siloBuilder.UseLocalhostClustering(
        builder.Configuration.GetValue("Silo:Port", 11111),
        builder.Configuration.GetValue("Silo:GatewayPort", 30000));
    if (streamTransport == "push")
    {
        // PUSH: no pulling agents at all — publish is a non-blocking channel write, one pump task per
        // subscriber delivers (into the grain's own turn, via a grain extension, for grain subscribers).
        // Streams:PushCapacity bounds each subscriber's backlog; overflow drops the incoming item and
        // logs a throttled counter (see PushStreamBus's backpressure paragraph).
        siloBuilder.AddPushStreams(
            StreamConstants.ProviderName,
            builder.Configuration.GetValue("Streams:PushCapacity", 10_000));
    }
    else
    {
        // Memory streams are PULL-based: pulling agents poll the in-memory queues every
        // GetQueueMessagesTimerPeriod (Orleans default 100ms). Every stream hop therefore adds
        // Uniform(0, period) latency — the table path pays it twice (sources → TableGrain,
        // tableDelta → SignalR bridge), which is exactly the 122ms p50 / 209ms p90 the 005-W9
        // benchmark measured vs Dapr's push-based 7ms. Streams:PullPeriodMs makes the cadence
        // tunable; default keeps Orleans' stock behavior.
        var pullPeriodMs = builder.Configuration.GetValue("Streams:PullPeriodMs", 100);
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName, configurator =>
            configurator.ConfigurePullingAgent(ob => ob.Configure(o =>
                o.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(pullPeriodMs))));
    }
    siloBuilder.AddMemoryGrainStorage(StreamConstants.PubSubStoreName);
    siloBuilder.AddJsonFileGrainStorage(StreamConstants.StorageName);

    // CRDT-as-a-plugin: a grain class living in a dynamically loaded plugin DLL (StreamsForge.Plugins.Crdt
    // merges CrdtDocGrain into one file under plugins/) is not automatically part of this silo's grain
    // manifest the way a project-referenced grain assembly is.
    //
    // TWO SEPARATE REGISTRATIONS ARE NEEDED — VERIFIED LIVE, THE HARD WAY. The obvious-looking one-liner,
    // `siloBuilder.Services.AddSerializer(b => b.AddAssembly(assembly))`, registers the assembly's
    // generated `TypeManifestProviderAttribute` class with the SERIALIZER's type manifest (codecs,
    // copiers, interface-to-proxy invoker mappings) — necessary, but NOT sufficient. Booting a real host
    // with only that call, creating a crdt-kind source still threw
    // `System.ArgumentException: Could not find an implementation for interface
    // StreamsForge.Abstractions.ICrdtDocGrain` out of
    // `Orleans.GrainInterfaceTypeToGrainTypeResolver.GetGrainType` — the interface-to-CONCRETE-GRAIN-CLASS
    // mapping is a silo-local concern read from `IOptions<Orleans.Configuration.GrainTypeOptions>`
    // (`Classes`/`Interfaces`, both `HashSet<Type>`), which a project-referenced grain assembly gets
    // populated into automatically (the referencing project's own Orleans-generated startup code calls
    // `services.Configure<GrainTypeOptions>(...)` for every grain type IT was compiled against) but a
    // `LoadFromAssemblyPath`'d plugin assembly — never compiled against by the Host — does not. The fix:
    // reflect over each loaded plugin assembly for concrete, non-abstract classes assignable to
    // `Orleans.IGrain` (the common marker every `IGrainWithXKey` grain interface extends) and add both the
    // class and its own grain interface(s) to `GrainTypeOptions` directly. Loop over
    // `StreamsForgePlugins.Loaded` rather than hard-coding this one plugin because this must work for ANY
    // future grain-carrying plugin, not just this one.
    //
    // ORDERING, VERIFIED: this lambda is the CONFIGURE action UseOrleans defers — it does not run when
    // UseOrleans(...) is called (that line sits ABOVE the LoadFrom loop, further down this file, in source
    // order) but when builder.Build() actually assembles the silo's service provider. LoadFrom runs before
    // Build() unconditionally, so StreamsForgePlugins.Loaded is already populated by the time this foreach
    // executes — confirmed live: a source built with the crdt plugin ABSENT still boots and both loops
    // below are simply empty; a source built WITH the plugin loaded activates CrdtDocGrain from the merged
    // plugins/StreamsForge.Plugins.Crdt.dll, not from a project-referenced copy (there isn't one any more
    // — the host no longer references StreamsForge.Connectors.Crdt directly).
    siloBuilder.Services.AddSerializer(b =>
    {
        foreach (var p in StreamsForge.AppCore.Plugins.StreamsForgePlugins.Loaded)
        {
            b.AddAssembly(p.Assembly);
        }
    });
    siloBuilder.Services.Configure<Orleans.Configuration.GrainTypeOptions>(o =>
    {
        foreach (var p in StreamsForge.AppCore.Plugins.StreamsForgePlugins.Loaded)
        {
            foreach (var type in p.Assembly.GetTypes())
            {
                if (!type.IsClass || type.IsAbstract || !typeof(Orleans.IGrain).IsAssignableFrom(type))
                {
                    continue;
                }

                o.Classes.Add(type);
                foreach (var iface in type.GetInterfaces())
                {
                    // Orleans' OWN grain-marker interfaces (IGrain, IGrainWithStringKey, IAddressable, …)
                    // live in the Orleans namespace and are already known to the silo — only the plugin's
                    // OWN grain interface (ICrdtDocGrain) needs adding here.
                    if (typeof(Orleans.IGrain).IsAssignableFrom(iface) && iface.Namespace?.StartsWith("Orleans", StringComparison.Ordinal) != true)
                    {
                        o.Interfaces.Add(iface);
                    }
                }
            }
        }
    });

    // Plan 011 wave D1 — HOW LONG AN IDLE SHARD STAYS RESIDENT.
    //
    // TableShardGrain is the one grain in the table path that does NOT call DelayDeactivation, so it is
    // the one grain Orleans' activation collector can actually reclaim — and how quickly it does is the
    // difference between "sharding bounds resident memory" and "sharding is a nice API". Orleans'
    // default CollectionAge is 15 minutes, which is a reasonable default for grains generally and far
    // too long for a soak run or a live check to observe anything, so it is configurable here.
    //
    // Shards:IdleSeconds sets the shard class's own collection age. Shards:QuantumSeconds sets the
    // silo-wide scan interval, which Orleans requires to be strictly SMALLER than any collection age;
    // its default (60s) is left alone unless asked for, since it applies to every grain type. Both are
    // pass-through knobs with no behavioral default change: at the default 120s a shard becomes eligible
    // after two minutes idle and is collected on the next 60s scan.
    var shardIdleSeconds = builder.Configuration.GetValue("Shards:IdleSeconds", 120);
    var shardQuantumSeconds = builder.Configuration.GetValue("Shards:QuantumSeconds", 0);
    siloBuilder.Configure<Orleans.Configuration.GrainCollectionOptions>(o =>
    {
        if (shardQuantumSeconds > 0)
        {
            o.CollectionQuantum = TimeSpan.FromSeconds(shardQuantumSeconds);
        }
        if (shardIdleSeconds > 0)
        {
            o.ClassSpecificCollectionAge[typeof(StreamsForge.Host.Grains.TableShardGrain).FullName!] =
                TimeSpan.FromSeconds(shardIdleSeconds);
        }
    });
});

builder.Services.AddStreamsForgeApi(builder.Configuration);
builder.Services.AddOrleansFacades();
builder.Services.AddHostedService<GeneratorSupervisorService>();
// Plan 008 W4: drives SourceIngressBuffer.DrainAsync — without it a buffered push is admitted and
// never published. See the service's own doc comment.
builder.Services.AddHostedService<IngestDrainPumpService>();
builder.Services.AddHostedService<StreamBridgeService>();
// Plan 009 B2: a second, independent consumer at the same stream seam as StreamBridgeService —
// fire-and-forget republishes pipeline results / table deltas to NATS for entities with Sinks
// configured. See the service's own doc comment.
builder.Services.AddHostedService<NatsPublisherService>();

// Plan 025 G1: gRPC services + protos live in shared/StreamsForge.Api now (Grpc/**), so this is
// AddGrpc() plus the ONE dependency of theirs that is genuinely runtime-specific — the live per-entity
// subscription primitive the two streaming services need (Orleans streams here, the Dapr topic fan-out
// there). Everything else they take (ICatalogFacade, ITableReadFacade, IIngressFacade, AccessGuard) is
// already registered by AddStreamsForgeApi/AddOrleansFacades above.
builder.Services.AddStreamsForgeGrpc();
builder.Services.AddSingleton<IEntityStreamFacade, OrleansEntityStreamFacade>();

// Plan 026 D6 — host defaults for the disk-backed replay tier (Replay:SegmentBytes / RetainMs /
// MaxBytesPerStream); the directory defaults to DataDir/replay, next to the grain state it belongs with.
builder.Services.AddOptions<StreamsForge.AppCore.Streaming.ReplayOptions>()
    .Bind(builder.Configuration.GetSection("Replay"))
    .PostConfigure(o => o.Directory ??= Path.Combine(builder.Configuration["DataDir"] ?? "./data", "replay"));

// Plan 014-I: the out-of-core database connectors' only call site. InboundTransports/PolledTransports
// both document "before any source starts" as the registration deadline; nothing in this process can
// start a source before builder.Build() returns and the hosted services below get to Run(), so anywhere
// before this line satisfies it — here, immediately before Build(), keeps it visibly paired with the
// rest of the transport wiring above rather than buried at the top of the file.
DatabaseConnectors.RegisterAll();
// Quant (pricing scalars) and Fix (the `fix`/`fix-duplex` transports) used to register here directly;
// they are now install-time server plugins under plugins/, loaded by StreamsForgePlugins.LoadFrom below
// like any other out-of-tree connector.

// Out-of-tree connectors, installed rather than referenced: every IStreamsForgePlugin in `plugins/` next
// to the binaries (or wherever `Plugins:Path` points) registers its own kinds here, at the same
// "before any source starts" deadline the three lines above satisfy — and AFTER them, so a plugin
// shipping a kind name a built-in already owns loses to the built-in instead of shadowing it. The report
// is logged, never thrown: a plugin that fails to load must not keep this host from starting, and the
// line is the only diagnostic an operator has for "I copied the DLL and nothing happened".
foreach (var line in StreamsForge.AppCore.Plugins.StreamsForgePlugins.LoadFrom(builder.Configuration["Plugins:Path"]))
{
    Console.WriteLine($"[plugins] {line}");
}
// A plugin that implements IStreamsForgeWebPlugin (StreamsForge.Api) gets its two host hooks here:
// services now, endpoints after Build(). Loaded plugins are also what a runtime adds to its own type
// manifest (Orleans: grains/serializers) — see the UseOrleans block.
StreamsForge.Api.Plugins.StreamsForgePluginHosting.ConfigureServices(builder.Services, builder.Configuration);

// Plan 016 wave 5: this instance's directory of known peers, read from config and installed into the
// process-wide PeerDirectory before the host starts serving. "Discovery:Peers" (a section, not a flat
// key) so it binds as an ARRAY — the shape .NET configuration gives every provider for free, including
// the one every live check in this repo actually uses: `--Discovery:Peers:0:Name foo
// --Discovery:Peers:0:RestEndpoint http://host:port` on the command line, or DISCOVERY__PEERS__0__NAME /
// DISCOVERY__PEERS__0__RESTENDPOINT as env vars, bind identically to the equivalent appsettings.json
// `{ "Discovery": { "Peers": [ { "Name": "...", "RestEndpoint": "...", "GrpcEndpoint": "..." } ] } }`.
// Binding straight onto PeerRecord (not a separate DTO) keeps this a one-liner; the registry-owned
// fields (InstanceId/LastSeenAtMs/LastError/Info) simply stay at their zero values unless a probe sets
// them later, which is exactly the "configured but never seen" state PeerRecord already documents.
PeerDirectory.Configure(builder.Configuration.GetSection("Discovery:Peers").Get<List<PeerRecord>>() ?? []);

// Plan 016 wave 6: named external endpoints, read from the SAME configuration providers by the same
// rules — `--Endpoints:primary-oltp=host:5432` on the command line, Endpoints__PRIMARY_OLTP as an env
// var, or an "Endpoints" object in appsettings.json. A flat name->value map rather than a section of
// objects, because that is all an alias is. Deliberately NOT read from the catalog: a document that
// carried environment-specific endpoints would defeat the indirection it is asking for, and keeping the
// map here is what lets one exported catalog import into prod and dev alike.
NamedEndpoints.Configure(
    builder.Configuration.GetSection("Endpoints").GetChildren()
        .Where(c => c.Value is not null)
        .Select(c => new KeyValuePair<string, string>(c.Key, c.Value!)));

var app = builder.Build();

// Re-emitted through the real logging pipeline (OutboundTls.Configure ran before it existed). This is
// a development-only escape hatch that disables outbound certificate validation entirely, so it must
// be visible in whatever log an operator actually reads.
if (builder.Configuration.GetValue(OutboundTls.AcceptAnyCertificateKey, false))
{
    app.Logger.LogWarning(
        "{Key} is TRUE: every outbound HTTPS/gRPC connection from this instance accepts ANY server "
      + "certificate. Development only.",
        OutboundTls.AcceptAnyCertificateKey);
}

// Host-specific facts StreamsForgeApiOptions carries so the shared endpoints stay byte-identical
// across runtimes (plan 005 W3, decision D-B). Values below reproduce exactly what the pre-W3
// Program.cs resolved inline.
var apiOptions = new StreamsForgeApiOptions(
    // Plan 025 G1: AppContext.BaseDirectory, not ContentRootPath — the .proto files moved into
    // shared/StreamsForge.Api, which copies them into every referencing project's OUTPUT directory
    // (see that csproj's <Content> item). ContentRootPath is this project's SOURCE directory under
    // `dotnet run`, and there is no Protos/ there any more. The single-file publish path is unchanged:
    // Publish.props still embeds the same two files and MetaEndpoints still falls back to them.
    ProtosDir: Path.Combine(AppContext.BaseDirectory, "Protos"),
    GrpcPort: grpcPort,
    // Plan 025 G1: the list is no longer written out here — StreamsForgeGrpc.StaticServiceNames is the
    // one definition, next to the MapStreamsForgeGrpc call that maps exactly those services, so what
    // /api/meta/instance advertises cannot drift from what is actually mapped.
    GrpcStaticServices: StreamsForgeGrpc.StaticServiceNames,
    DocsFilePath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Docs:File"] ?? Path.Combine("..", "..", "docs", "index.html"))),
    SpaDistPath: Path.GetFullPath(Path.Combine(
        app.Environment.ContentRootPath,
        app.Configuration["Web:Dist"] ?? Path.Combine("..", "..", "..", "web", "dist"))),
    Flavor: "orleans",
    // Plan 016 wave 5: same config key JsonFileGrainStorage already reads (see that class's
    // AddJsonFileGrainStorage extension) — one DataDir, one identity file living next to the grain state
    // it is already the default for, not a second directory setting nobody would think to keep in sync.
    DataDir: app.Configuration["DataDir"] ?? "./data",
    InstanceName: app.Configuration["InstanceName"] ?? "",
    Version: app.Configuration["Version"] ?? "");

app.MapStreamsForgeApi(apiOptions);
app.MapPluginEndpoints();

// gRPC control plane + streaming (shared/StreamsForge.Api/Protos/streamsforge.proto) — served on the
// HTTP/2-only endpoint configured above (Grpc:Port, default 5299); doesn't share the REST/SignalR/SPA
// port. All seven services (tier 1 control plane/streaming/ingest + tier 2 dynamic reflection and the
// generic typed-streaming RPC) are mapped by the shared extension, whose own doc explains why the list
// lives there rather than being written out once per host.
app.MapStreamsForgeGrpc();

app.Lifetime.ApplicationStarted.Register(() => _ = InitializeGrainsAsync(app.Services));

app.Run();

static async Task InitializeGrainsAsync(IServiceProvider services)
{
    try
    {
        var client = services.GetRequiredService<IClusterClient>();
        await client.GetGrain<IUserStoreGrain>(StreamConstants.UsersKey).EnsureInitializedAsync();

        // Plan 021 D1/item-3 — seeding still applies to the DEFAULT environment exactly as before (the
        // environment directory always lists it first — see IEnvironmentRegistryGrain.ListAsync), but the
        // boot path must also resume already-Running generators/connectors/pipelines/tables in every OTHER
        // environment that was ever created, not just default. On an instance that has never created one,
        // ListAsync returns exactly [default] and this loop is one call, byte-identical to the pre-021 line
        // it replaces (EnvKeys.Qualify("", RegistryKey) == RegistryKey).
        var environments = await client.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey).ListAsync();
        foreach (var env in environments)
        {
            await client.RegistryFor(EnvKeys.Normalize(env.Name)).EnsureInitializedAsync();
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[StreamsForge.Host] grain initialization failed: {ex}");
    }
}
