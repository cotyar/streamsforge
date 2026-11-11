using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.Api.Plugins;
using StreamsForge.Host.Facades;

namespace StreamsForge.Plugins.Crdt;

/// <summary>
/// Installs the Orleans CRDT document runtime (<c>CrdtDocGrain</c>, dispatched to via
/// <c>SourceKindDispatch.ActorKind.Crdt</c>) as a server plugin instead of a host ProjectReference.
/// Merged to one self-contained DLL by <c>plugins/ILRepack.Plugins.targets</c> (StreamsForge.Connectors.Crdt
/// + Ycs, which has no third-party dependency of its own) — an
/// operator installs this by copying ONE file
/// into the host's <c>plugins/</c> directory (or wherever <c>Plugins:Path</c> points).
///
/// <para><see cref="Register"/> is a no-op: unlike a transport-registry-based connector (Fix, the database
/// kinds), a grain needs no process-wide registration call of its own — Orleans discovers grain CLASSES
/// from the assemblies it is told about (see <c>Program.cs</c>'s <c>AddSerializer(b =&gt;
/// b.AddAssembly(...))</c> call over every loaded plugin's assembly). What this plugin's second tier
/// (<see cref="IStreamsForgeWebPlugin.ConfigureServices"/>) DOES need to do is replace the host's default
/// <c>DisabledCrdtFacade</c> registration with the real <see cref="OrleansCrdtFacade"/> — "last
/// registration wins" for singleton resolution, and this hook runs after
/// <c>OrleansFacadesExtensions.AddOrleansFacades</c> in <c>Program.cs</c>'s call order.</para>
/// </summary>
public sealed class CrdtPlugin : IStreamsForgeWebPlugin
{
    public string Name => "crdt";

    public void Register()
    {
    }

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration) =>
        services.AddSingleton<ICrdtFacade, OrleansCrdtFacade>();
}
