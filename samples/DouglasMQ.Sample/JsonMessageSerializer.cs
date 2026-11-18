using System.Text.Json;
using DouglasMQ.Serialization;

namespace DouglasMQ.Sample;

public sealed class JsonMessageSerializer : ISerializer
{
	public object? Deserialize(string data, Type type) => JsonSerializer.Deserialize(data, type);

	public string Serialize<T>(T data) => JsonSerializer.Serialize(data);
}
