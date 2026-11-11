using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 009 B1: live cluster coverage for the Orleans <c>nats</c>-kind dispatch — sibling to
/// orleans/tests/StreamsForge.Host.Tests/ConnectorGrainClusterTests.cs's file-kind coverage (not mine to
/// edit — new-files-only convention). There is no NATS server in this sandbox (verified once — see the
/// plan's own note), so this deliberately targets an UNREACHABLE broker and asserts the grain degrades
/// to an "error" status instead of crashing the silo — the one live check this wave CAN make without a
/// broker. Message→row mapping, coercion, and reconnect/backoff logic are covered by
/// NatsSubscriberCoreTests.cs against a fake <c>INatsMessageSource</c> instead.</summary>
internal sealed class NatsConnectorTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddMemoryStreams(StreamConstants.ProviderName);
        siloBuilder.AddPubSubMemoryGrainStorage(StreamConstants.PubSubStoreName);
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
    }
}

internal sealed class NatsConnectorTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
        clientBuilder.AddMemoryStreams(StreamConstants.ProviderName);
}

public sealed class ConnectorGrainNatsClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<NatsConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<NatsConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private static SourceDefinition MakeNatsSource(string name, string url) => new()
    {
        Name = name,
        Kind = SourceKinds.Nats,
        Enabled = true,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("price", FieldType.Double)],
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = url, Subject = "trades.>", Format = "json" },
        },
    };

    [Fact]
    public async Task Nats_kind_connector_degrades_to_error_status_against_an_unreachable_broker_without_crashing()
    {
        var name = "nats_conn_" + Guid.NewGuid().ToString("n")[..8];
        // Port 1 is a privileged/reserved port essentially guaranteed to refuse a connection immediately
        // on any dev machine or CI runner — no real NATS broker exists in this sandbox to dial instead.
        var grain = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);

        try
        {
            await grain.StartAsync(MakeNatsSource(name, "nats://127.0.0.1:1"));

            var status = await PollUntilAsync(
                () => grain.GetStatusAsync(),
                s => s.LastStatus == "error",
                deadlineSeconds: 20);

            Assert.Equal("error", status.LastStatus);
            Assert.NotNull(status.LastError);
            Assert.Equal(name, status.SourceName);

            // The silo/grain itself must still be alive and answering — a crashed grain would fault this call.
            var again = await grain.GetStatusAsync();
            Assert.Equal(name, again.SourceName);
        }
        finally
        {
            await grain.StopAsync();
        }

        // Stopped cleanly: a further status call still works (grain not corrupted) and stays whatever it
        // last was — StopAsync does not reset status, only cancels the subscriber.
        var afterStop = await grain.GetStatusAsync();
        Assert.Equal(name, afterStop.SourceName);
    }

    [Fact]
    public async Task Registry_dispatches_nats_kind_to_the_connector_grain_not_the_generator()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.EnsureInitializedAsync();

        var name = "nats_conn_registry_" + Guid.NewGuid().ToString("n")[..8];
        try
        {
            await registry.UpsertSourceAsync(MakeNatsSource(name, "nats://127.0.0.1:1"));

            // Kind dispatch happened (a connector grain got StartAsync'd) as soon as the status leaves
            // "never" — degrading to "error" against the unreachable broker is the expected, honest
            // outcome, and proves ConnectorGrain (not IGeneratorGrain) is driving this source.
            var status = await PollUntilAsync(
                () => _cluster.GrainFactory.GetGrain<IConnectorGrain>(name).GetStatusAsync(),
                s => s.LastStatus != "never",
                deadlineSeconds: 20);

            Assert.NotEqual("never", status.LastStatus);
        }
        finally
        {
            await registry.DeleteSourceAsync(name);
        }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }
}
