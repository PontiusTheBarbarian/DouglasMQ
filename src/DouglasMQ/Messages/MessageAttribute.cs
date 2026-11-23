using System;

namespace DouglasMQ
{
	/// <summary>
	/// Declares where a message is published: the exchange and the routing key. Required on every
	/// type passed to <see cref="IPublisher.PublishAsync{TEvent}"/> or <see cref="IPublisher.SendAsync{TCommand}"/>;
	/// not needed for messages that are only consumed.
	/// </summary>
	/// <remarks>
	/// The publisher does not declare the exchange. It must already exist (a consumer's
	/// <see cref="HandlerAttribute"/> declares it at startup), otherwise the broker rejects the publish.
	/// </remarks>
	/// <example>
	/// <code>
	/// [Message("orders.exchange", routingKey: "order.placed")]
	/// public class OrderPlaced : IEvent { ... }
	/// </code>
	/// </example>
	[AttributeUsage(AttributeTargets.Class,
		AllowMultiple = false,
		Inherited = true)]
	public sealed class MessageAttribute : Attribute
	{
		/// <summary>
		/// Initialises a new <see cref="MessageAttribute"/>.
		/// </summary>
		/// <param name="exchangeName">The name of the exchange this message is published to.</param>
		/// <param name="routingKey">The routing key used when publishing. Defaults to <c>"*"</c>.</param>
		public MessageAttribute(string exchangeName,
			string routingKey = "*")
		{
			ExchangeName = exchangeName;
			RoutingKey = routingKey;
		}

		/// <summary>
		/// The name of the exchange this message is published to.
		/// </summary>
		public string ExchangeName { get; }

		/// <summary>
		/// The routing key the message is published with.
		/// </summary>
		public string RoutingKey { get; }
	}
}
