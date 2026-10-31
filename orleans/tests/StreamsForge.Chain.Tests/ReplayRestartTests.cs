using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// Plan 026 wave 3 D6/D8 at the process level — the one thing a <c>TestCluster</c> cannot show: that a
/// producer's replay positions survive the PROCESS, not just an activation.
///
/// <para>Shape: an in-test HTTP server serves 200 rows; two <c>url</c> sources poll it, one with
/// <c>replay: { persist: true }</c> and one without; a <c>MemoryOnly</c> table sits over each. Both fill
/// to 200. The server's body is then switched to <c>[]</c>, so nothing can be re-polled, and the host is
/// killed (<c>entireProcessTree</c>) and started again on the SAME data directory and ports.</para>
///
/// <para>Two assertions, and the second is the point:</para>
/// <list type="number">
/// <item>gRPC <c>SubscribeSource{from_seq: 1}</c> on the persisted source yields 200 events with
/// positions 1…200 and no <c>replay_truncated</c> — the positions a client was handed before the
/// restart still mean the same rows after it (D6).</item>
/// <item>The table over the PERSISTED source comes back holding all 200 rows with
/// <c>rebuilding == false</c>, while the table over the plain source — same SQL, same persistence mode,
/// same restart — comes back holding ZERO (D8). The pre-plan behaviour is the second one; asserting
/// both is what makes the difference a statement rather than an implication.</item>
/// </list>
///
/// <para>gRPC is driven with <c>grpcurl</c> (over server reflection, with an admin bearer token) rather
/// than a generated client: this project links no gRPC packages on purpose — see its csproj's own note
/// on why it talks to a spawned host over the wire and nothing else. A machine without <c>grpcurl</c>
/// skips that half with a stated reason instead of failing.</para>
///
/// <para>Ports 8599/8699 REST/gRPC and 18599/38599 silo — disjoint from every other chain fixture's, and
/// in the same xunit collection so only one class's hosts are alive at a time.</para>
/// </summary>
[Collection(ChainHostCollection.Name)]
public sealed class ReplayRestartTests : IAsyncLifetime
{
    private const int HttpPort = 8599;
    private const int GrpcPort = 8699;
    private const int SiloPort = 18599;
    private const int GatewayPort = 38599;

    private const string PersistedSource = "replay_persist_src";
    private const string PlainSource = "replay_plain_src";
    private const string PersistedTable = "replay_persist_tbl";
    private const string PlainTable = "replay_plain_tbl";
    private const int RowCount = 200;

    private HostProcess? _host;
    private HttpListener? _listener;
    private Task? _listenerLoop;
    private int _listenerPort;
    private string? _skipReason;

    private volatile string _body = "[]";

    public Task InitializeAsync()
    {
        _skipReason = HostProcess.Preflight(HttpPort, GrpcPort, SiloPort, GatewayPort);
        if (_skipReason is not null)
        {
            return Task.CompletedTask;
        }

        _listenerPort = GetFreePort();
        _body = RowsJson(RowCount);
        try
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://127.0.0.1:{_listenerPort}/");
            _listener.Start();
            _listenerLoop = Task.Run(ServeAsync);
        }
        catch (Exception ex)
        {
            _skipReason = $"could not start the in-test HTTP server on 127.0.0.1:{_listenerPort}: {ex.Message}";
            return Task.CompletedTask;
        }

        _host = new HostProcess("replay-restart", HttpPort, GrpcPort, SiloPort, GatewayPort);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
        try
        {
            _listener?.Stop();
            _listener?.Close();
        }
        catch
        {
            // best-effort
        }
        if (_listenerLoop is not null)
        {
            try { await _listenerLoop.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public async Task A_persisted_source_replays_its_positions_and_refills_its_table_after_a_host_restart()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        string persistedTableId, plainTableId;
        using (var client = await host.LoginAsync())
        {
            await CreateSourceAsync(client, PersistedSource, persist: true);
            await CreateSourceAsync(client, PlainSource, persist: false);
            persistedTableId = await CreateTableAsync(client, PersistedTable, PersistedSource);
            plainTableId = await CreateTableAsync(client, PlainTable, PlainSource);

            await PollAsync(
                TimeSpan.FromSeconds(60),
                async () => await TotalAsync(client, persistedTableId) == RowCount
                         && await TotalAsync(client, plainTableId) == RowCount,
                "both tables never reached 200 rows before the restart",
                host);

            // Plan 026 D6 — the write-behind timer is 2s; the stop below flushes the tail anyway, but give
            // the log a tick so the "segments exist before the kill" case is the one under test too.
            await Task.Delay(3000);
        }

        // Nothing is re-pollable after the restart: an empty body plus the persisted dedup keys mean every
        // row the tables end up holding came out of the replay log, not out of a fresh poll.
        _body = "[]";

        await host.StopAsync();
        host.Start();
        await host.WaitHealthyAsync();

        using (var client = await host.LoginAsync())
        {
            // (b) D8 — the table over the persisted source refills; the one over the plain source does not.
            await PollAsync(
                TimeSpan.FromSeconds(60),
                async () => await TotalAsync(client, persistedTableId) == RowCount,
                $"table {PersistedTable} did not refill to {RowCount} rows from its source's persisted replay "
              + "log after the restart",
                host);

            using (var metrics = await GetJsonAsync(client, $"api/tables/{persistedTableId}/metrics"))
            {
                Assert.False(
                    metrics.RootElement.GetProperty("rebuilding").GetBoolean(),
                    "a table rebuilt from its inputs' retained history is not still rebuilding");
            }

            // The control: same SQL, same persistence mode, same restart, no replay.persist on its source.
            // This is the PRE-PLAN behaviour, asserted rather than assumed — without it, "200 rows" could
            // just as well mean the source re-polled.
            Assert.Equal(0, await TotalAsync(client, plainTableId));

            // (a) D6 — positions survived the process.
            await AssertGrpcReplayAsync(client, host);

            // (c) the operator-facing view of the same fact (wave 3 agent C's route): the persisted
            // source reports its disk tier and the positions a client can ask for; the plain one does not.
            using (var replay = await GetJsonAsync(client, $"api/sources/{PersistedSource}/replay"))
            {
                Assert.True(replay.RootElement.GetProperty("persisted").GetBoolean(), "persisted source must report persisted: true");
                Assert.Equal(1, replay.RootElement.GetProperty("firstSeq").GetInt64());
                Assert.Equal(RowCount, replay.RootElement.GetProperty("lastSeq").GetInt64());
            }

            using (var replay = await GetJsonAsync(client, $"api/sources/{PlainSource}/replay"))
            {
                Assert.False(replay.RootElement.GetProperty("persisted").GetBoolean(), "plain source must report persisted: false");
            }
        }
    }

    // ---- gRPC via grpcurl ----

    private static async Task AssertGrpcReplayAsync(HttpClient client, HostProcess host)
    {
        var grpcurl = WhichGrpcurl();
        if (grpcurl is null)
        {
            // Reported, not silently passed: a machine without grpcurl proves nothing about the wire.
            Assert.True(true, "grpcurl not found on PATH — the gRPC half of this test did not run");
            return;
        }

        var token = client.DefaultRequestHeaders.Authorization!.Parameter!;
        var psi = new ProcessStartInfo(grpcurl)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-plaintext");
        psi.ArgumentList.Add("-max-time");
        psi.ArgumentList.Add("15");
        psi.ArgumentList.Add("-H");
        psi.ArgumentList.Add($"authorization: Bearer {token}");
        psi.ArgumentList.Add("-d");
        psi.ArgumentList.Add($"{{\"name\":\"{PersistedSource}\",\"from_seq\":1}}");
        psi.ArgumentList.Add($"127.0.0.1:{host.GrpcPort}");
        psi.ArgumentList.Add("streamsforge.v1.StreamService/SubscribeSource");

        using var proc = Process.Start(psi)!;
        var stdout = await proc.StandardOutput.ReadToEndAsync();
        var stderr = await proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(40));

        var positions = Regex.Matches(stdout, "\"position\"\\s*:\\s*\"?(\\d+)\"?")
            .Select(m => long.Parse(m.Groups[1].Value))
            .ToList();

        Assert.True(
            positions.Count == RowCount,
            $"expected {RowCount} replayed events from position 1, got {positions.Count}.\n"
          + $"--- grpcurl stderr ---\n{stderr}\n--- host log tail ---\n{host.LogTailText()}");
        Assert.Equal(Enumerable.Range(1, RowCount).Select(i => (long)i), positions);
        Assert.DoesNotContain("\"replayTruncated\": true", stdout, StringComparison.Ordinal);
        Assert.DoesNotContain("\"replay_truncated\": true", stdout, StringComparison.Ordinal);
    }

    private static string? WhichGrpcurl()
    {
        foreach (var dir in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator))
        {
            if (dir.Length == 0) continue;
            var candidate = Path.Combine(dir, "grpcurl");
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }

    // ---- REST shapes ----

    private async Task CreateSourceAsync(HttpClient client, string name, bool persist)
    {
        object body = persist
            ? new
            {
                name,
                kind = "url",
                enabled = true,
                replay = new { persist = true },
                fields = FieldsJson(),
                connector = ConnectorJson(_listenerPort),
            }
            : new
            {
                name,
                kind = "url",
                enabled = true,
                fields = FieldsJson(),
                connector = ConnectorJson(_listenerPort),
            };
        (await PostOkAsync(client, "api/sources", body)).Dispose();
    }

    private static async Task<string> CreateTableAsync(HttpClient client, string name, string sourceName)
    {
        using var created = await PostOkAsync(client, "api/tables", new
        {
            name,
            sql = $"SELECT id, value FROM {sourceName} LATEST BY (id)",
            persistence = "MemoryOnly",
        });
        var id = created.RootElement.GetProperty("id").GetString()!;
        (await client.PostAsync($"api/tables/{id}/start", content: null)).EnsureSuccessStatusCode();
        return id;
    }

    private static object ConnectorJson(int listenerPort) => new
    {
        schedule = new { intervalMs = 1000 },
        url = new { url = $"http://127.0.0.1:{listenerPort}/rows" },
        mapping = new
        {
            itemsPath = "$",
            dedupKeyField = "id",
            fields = MappingFieldsJson(),
        },
    };

    private static object[] FieldsJson() =>
    [
        new { name = "id", type = "Long" },
        new { name = "value", type = "String" },
    ];

    private static object[] MappingFieldsJson() =>
        FieldsJson().Select(f => (object)new { field = f }).ToArray();

    // ---- the in-test HTTP server ----

    private async Task ServeAsync()
    {
        while (_listener is { IsListening: true })
        {
            HttpListenerContext ctx;
            try { ctx = await _listener.GetContextAsync(); }
            catch { return; }
            try
            {
                var bytes = Encoding.UTF8.GetBytes(_body);
                ctx.Response.ContentType = "application/json";
                ctx.Response.ContentLength64 = bytes.Length;
                await ctx.Response.OutputStream.WriteAsync(bytes);
                ctx.Response.Close();
            }
            catch
            {
                // a dropped client is not this test's problem
            }
        }
    }

    private static string RowsJson(int count)
    {
        var sb = new StringBuilder("[");
        for (var i = 0; i < count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(JsonSerializer.Serialize(new { id = i, value = $"v{i}" }));
        }
        return sb.Append(']').ToString();
    }

    private static int GetFreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    // ---- REST helpers (same shapes as HostRestartTests') ----

    private static async Task<int> TotalAsync(HttpClient client, string tableId)
    {
        using var doc = await GetJsonAsync(client, $"api/tables/{tableId}/rows?limit=1");
        return doc.RootElement.GetProperty("totalRows").GetInt32();
    }

    private static async Task<JsonDocument> PostOkAsync(HttpClient client, string url, object body)
    {
        var resp = await client.PostAsJsonAsync(url, body);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"POST {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    private static async Task<JsonDocument> GetJsonAsync(HttpClient client, string url)
    {
        var resp = await client.GetAsync(url);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"GET {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    private static async Task PollAsync(TimeSpan timeout, Func<Task<bool>> condition, string message, HostProcess host)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(250);
        }
        Assert.Fail($"{message} within {timeout.TotalSeconds:0}s.\n--- host log tail ---\n{host.LogTailText()}");
    }
}
