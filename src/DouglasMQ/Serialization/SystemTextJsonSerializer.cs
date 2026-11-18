using System;
using System.Text.Json;

namespace DouglasMQ.Serialization
{
	/// <summary>
	/// Default <see cref="ISerializer"/> backed by <see cref="System.Text.Json"/>. Used automatically
	/// when no serializer is registered.
	/// </summary>
	/// <remarks>
	/// Uses <see cref="JsonSerializerDefaults.Web"/> unless options are supplied: camelCase property
	/// names on the wire and case-insensitive reading, which matches what most non-.NET consumers
	/// and tools expect.
	/// </remarks>
	public sealed class SystemTextJsonSerializer : ISerializer
	{
		private readonly JsonSerializerOptions _options;

		/// <summary>
		/// Creates a serializer using <see cref="JsonSerializerDefaults.Web"/>.
		/// </summary>
		public SystemTextJsonSerializer()
			: this(new JsonSerializerOptions(JsonSerializerDefaults.Web))
		{
		}

		/// <summary>
		/// Creates a serializer with custom options (converters, naming policy, source-generated
		/// type info and so on).
		/// </summary>
		public SystemTextJsonSerializer(JsonSerializerOptions options)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		/// <inheritdoc />
		public string Serialize<T>(T data) => JsonSerializer.Serialize(data, _options);

		/// <inheritdoc />
		/// <exception cref="JsonException">The payload is not valid JSON for <paramref name="type"/>.</exception>
		public object? Deserialize(string data, Type type) => JsonSerializer.Deserialize(data, type, _options);
	}
}
