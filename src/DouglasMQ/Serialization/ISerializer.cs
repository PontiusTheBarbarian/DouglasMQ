using System;
using System.Text;

namespace DouglasMQ.Serialization
{
	/// <summary>
	/// Defines the serialization contract used by DouglasMQ to encode and decode message bodies.
	/// Implement this interface to plug in any serialization library (e.g. System.Text.Json, Newtonsoft.Json, MessagePack).
	/// Register the implementation via <c>WithSerializer</c> on <see cref="DouglasMQ.DouglasMQBuilder"/>
	/// or via <c>AddSerializer</c> on the Microsoft DI extension methods.
	/// </summary>
	/// <remarks>
	/// DouglasMQ always calls the byte-based members, <see cref="SerializeToBytes{T}"/> and
	/// <see cref="DeserializeFromBytes"/>. By default they UTF-8 encode and decode around the string-based
	/// members, so a text serializer only has to implement <see cref="Serialize{T}"/> and
	/// <see cref="Deserialize"/>. Override the byte-based members to skip that intermediate string, or
	/// to use a binary wire format.
	/// </remarks>
	public interface ISerializer
	{
		/// <summary>
		/// Serializes <paramref name="data"/> to a JSON string (or other text wire format).
		/// </summary>
		/// <typeparam name="T">The concrete message type being serialized.</typeparam>
		/// <param name="data">The message instance to serialize.</param>
		/// <returns>A string representation of the message suitable for transmission over the broker.</returns>
		string Serialize<T>(T data);

		/// <summary>
		/// Deserializes a wire-format string back to an instance of <paramref name="type"/>.
		/// </summary>
		/// <param name="data">The string received from the broker.</param>
		/// <param name="type">The target type to deserialize into.</param>
		/// <returns>The deserialized object, or <c>null</c> if the data cannot be deserialized.</returns>
		object? Deserialize(string data, Type type);

		/// <summary>
		/// Serializes <paramref name="data"/> to the bytes sent as the message body. Defaults to the
		/// UTF-8 encoding of <see cref="Serialize{T}"/>.
		/// </summary>
		/// <typeparam name="T">The concrete message type being serialized.</typeparam>
		/// <param name="data">The message instance to serialize.</param>
		byte[] SerializeToBytes<T>(T data) => Encoding.UTF8.GetBytes(Serialize(data));

		/// <summary>
		/// Deserializes a message body to an instance of <paramref name="type"/>. Defaults to
		/// <see cref="Deserialize"/> over the UTF-8 decoded body.
		/// </summary>
		/// <param name="data">The message body received from the broker.</param>
		/// <param name="type">The target type to deserialize into.</param>
		/// <returns>The deserialized object, or <c>null</c> if the data cannot be deserialized.</returns>
		object? DeserializeFromBytes(ReadOnlySpan<byte> data, Type type) => Deserialize(Encoding.UTF8.GetString(data), type);
	}
}
