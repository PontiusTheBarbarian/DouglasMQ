using System;
using System.Reflection;

namespace DouglasMQ
{
	/// <summary>
	/// Where messages of type <typeparamref name="TMessage"/> are published, read from its
	/// <see cref="MessageAttribute"/> once per type rather than on every publish.
	/// </summary>
	internal static class MessageRoute<TMessage>
	{
		private static readonly MessageAttribute? Attribute = typeof(TMessage).GetCustomAttribute<MessageAttribute>(inherit: true);

		/// <summary>The AMQP <c>type</c> property stamped on published messages.</summary>
		public static readonly string? TypeName = typeof(TMessage).FullName;

		/// <summary>The message's <see cref="MessageAttribute"/>.</summary>
		/// <exception cref="InvalidOperationException"><typeparamref name="TMessage"/> has no <see cref="MessageAttribute"/>.</exception>
		public static MessageAttribute Require() => Attribute
			?? throw new InvalidOperationException(
				$"Message type '{typeof(TMessage).Name}' is missing the [Message] attribute.");
	}
}
