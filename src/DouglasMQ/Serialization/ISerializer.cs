using System;

namespace DouglasMQ.Serialization
{
	/// <summary>
	/// Defines the serialization contract used by DouglasMQ to encode and decode message bodies.
	/// Implement this interface to plug in any serialization library (e.g. System.Text.Json, Newtonsoft.Json, MessagePack).
	/// Register the implementation via <c>WithSerializer</c> on <see cref="DouglasMQ.DouglasMQBuilder"/>
	/// or via <c>AddSerializer</c> on the Microsoft DI extension methods.
	/// </summary>
	public interface ISerializer
	{
		/// <summary>
		/// Serializes <paramref name="data"/> to a JSON string (or other wire format).
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
	}
}
