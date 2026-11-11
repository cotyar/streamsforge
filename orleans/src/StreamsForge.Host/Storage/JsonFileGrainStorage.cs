using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Storage;

namespace StreamsForge.Host.Storage;

/// <summary>
/// Single-silo JSON-file grain storage. One file per grain state at
/// {dataDir}/state/{stateName}.{sanitized-grain-id}.json. ETag is a no-op (single silo).
/// </summary>
public sealed partial class JsonFileGrainStorage(string name, string dataDir) : IGrainStorage
{
    /// <summary>Plan 011 wave C: <c>WriteIndented</c> is deliberately OFF. This is a storage format, not a
    /// document — every table's state file is rewritten in full on every flush tick (see TableGrain's
    /// persistence-mode doc), and indentation on a wide row-per-line snapshot is pure per-flush bytes and
    /// per-flush serializer work for nobody's benefit. Reading is unaffected in both directions:
    /// <see cref="JsonSerializer"/> ignores insignificant whitespace, so state files written by any earlier
    /// (indented) build still deserialize through this same options object unchanged — see
    /// JsonFileGrainStorageTests' round-trip-an-indented-file test. If a human ever needs to read one,
    /// `jq .` on the file does the job without making every process pay for it.</summary>
    internal static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>The named provider identity, kept for diagnostics/logging.</summary>
    public string Name { get; } = name;

    private readonly string _stateDir = Path.Combine(dataDir, "state");

    [GeneratedRegex("[^A-Za-z0-9_.-]")]
    private static partial Regex InvalidFileChars();

    private string PathFor(string stateName, GrainId grainId)
    {
        Directory.CreateDirectory(_stateDir);
        var sanitized = InvalidFileChars().Replace(grainId.ToString() ?? "unknown", "_");
        return Path.Combine(_stateDir, $"{stateName}.{sanitized}.json");
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var path = PathFor(stateName, grainId);
        if (!File.Exists(path))
        {
            grainState.RecordExists = false;
            return;
        }

        await using var stream = File.OpenRead(path);
        var value = await JsonSerializer.DeserializeAsync<T>(stream, JsonOptions);
        if (value is null)
        {
            grainState.RecordExists = false;
            return;
        }

        grainState.State = value;
        grainState.RecordExists = true;
        grainState.ETag = "1";
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var path = PathFor(stateName, grainId);
        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, grainState.State, JsonOptions);
        grainState.RecordExists = true;
        grainState.ETag = "1";
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var path = PathFor(stateName, grainId);
        if (File.Exists(path))
        {
            File.Delete(path);
        }

        grainState.RecordExists = false;
        return Task.CompletedTask;
    }
}

public static class JsonFileGrainStorageSiloBuilderExtensions
{
    /// <summary>Registers JSON-file-backed grain storage as a named provider.</summary>
    public static ISiloBuilder AddJsonFileGrainStorage(this ISiloBuilder builder, string name)
    {
        return builder.ConfigureServices(services =>
        {
            services.AddGrainStorage(name, (sp, providerName) =>
            {
                var config = sp.GetRequiredService<IConfiguration>();
                var dataDir = config["DataDir"] ?? "./data";
                return new JsonFileGrainStorage(providerName, dataDir);
            });
        });
    }
}
