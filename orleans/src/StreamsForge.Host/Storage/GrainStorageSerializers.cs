using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.Storage;

namespace StreamsForge.Host.Storage;

/// <summary>
/// An Orleans <see cref="IGrainStorageSerializer"/> over System.Text.Json with exactly
/// <see cref="JsonFileGrainStorage"/>'s options, so a grain's state round-trips through memory storage the
/// same way it round-trips through the real file storage.
///
/// <para><b>Why it exists.</b> Orleans' default storage serializer (what <c>AddMemoryGrainStorage</c> uses
/// unless told otherwise) is <c>JsonGrainStorageSerializer</c>: Newtonsoft.Json with a <c>$type</c> name in
/// every document. Orleans 10 checks that name against its serializer type allow-list on READ, and our state
/// classes are plain STJ types, not <c>[GenerateSerializer]</c> ones — so the first read after a deactivation
/// threw <c>JsonSerializationException</c>, which then could not cross the silo boundary and surfaced as
/// <c>CodecNotFoundException(Newtonsoft.Json.JsonSerializationException)</c>. That was the six "pre-existing"
/// reactivation test failures (plan 022's found-and-not-fixed list), fixed 2026-11-11.</para>
/// </summary>
public sealed class SystemTextJsonGrainStorageSerializer : IGrainStorageSerializer
{
    public BinaryData Serialize<T>(T input) =>
        BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(input, JsonFileGrainStorage.JsonOptions));

    public T Deserialize<T>(BinaryData input) =>
        JsonSerializer.Deserialize<T>(input.ToMemory().Span, JsonFileGrainStorage.JsonOptions)!;
}

/// <summary>In-memory grain storage that never goes through Newtonsoft.Json (see
/// <see cref="SystemTextJsonGrainStorageSerializer"/> for why the Orleans default is avoided).</summary>
public static class NewtonsoftFreeMemoryGrainStorageExtensions
{
    /// <summary>Memory storage for OUR grain state: System.Text.Json, the same format and options as
    /// <see cref="JsonFileGrainStorage"/> (so a test on memory storage exercises production's serialization).</summary>
    public static ISiloBuilder AddJsonMemoryGrainStorage(this ISiloBuilder builder, string name) =>
        builder.AddMemoryGrainStorage(name, o => o.GrainStorageSerializer = new SystemTextJsonGrainStorageSerializer());

    /// <summary>Memory storage for ORLEANS' own state (the stream PubSub store): Orleans' binary
    /// serializer, which every Orleans-internal state type is generated for.</summary>
    public static ISiloBuilder AddPubSubMemoryGrainStorage(this ISiloBuilder builder, string name)
    {
        builder.AddMemoryGrainStorage(name);
        builder.Services.AddOptions<MemoryGrainStorageOptions>(name)
            .Configure<Serializer>((o, serializer) => o.GrainStorageSerializer = new OrleansGrainStorageSerializer(serializer));
        return builder;
    }
}
