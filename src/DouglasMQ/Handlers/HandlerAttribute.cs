using System;

namespace DouglasMQ
{
	/// <summary>
	/// Declares where a handler consumes from: the exchange to bind to, the queue to read, and the
	/// binding's routing key. DouglasMQ declares that topology at startup (durable exchange and
	/// queue, dead-lettering to <c>dlx.exchange</c>) and starts one consumer per decorated handler.
	/// </summary>
	/// <example>
	/// <code>
	/// [Handler(ExchangeType.direct, "orders.exchange", "order-processing", routingKey: "order.placed")]
	/// public class OrderPlacedHandler : IHandler&lt;OrderPlaced&gt; { ... }
	/// </code>
	/// </example>
	[AttributeUsage(AttributeTargets.Class,
		AllowMultiple = false,
		Inherited = true)]
	public class HandlerAttribute : Attribute
	{
		/// <summary>
		/// Initialises a new <see cref="HandlerAttribute"/>.
		/// </summary>
		/// <param name="exchangeType">The type of the exchange to declare and bind to.</param>
		/// <param name="exchangeName">The name of the exchange to declare and bind to.</param>
		/// <param name="queueName">
		/// The durable queue this handler consumes. Handlers sharing a queue name compete for its messages.
		/// </param>
		/// <param name="routingKey">
		/// The binding key between the exchange and the queue. Defaults to <c>"*"</c>. Ignored by fanout
		/// and headers exchanges.
		/// </param>
		public HandlerAttribute(
			ExchangeType exchangeType,
			string exchangeName,
			string queueName,
			string routingKey = "*")
		{
			ExchangeType = Topology.ToAmqpName(exchangeType);
			ExchangeName = exchangeName;
			QueueName = queueName;
			RoutingKey = routingKey;
			ConsumerTag = $"{exchangeName}.{queueName}.{routingKey}";
		}

		/// <summary>
		/// The exchange type as the AMQP protocol names it, e.g. <c>"direct"</c> or <c>"topic"</c>.
		/// </summary>
		public string ExchangeType { get; }

		/// <summary>
		/// The name of the exchange the queue is bound to.
		/// </summary>
		public string ExchangeName { get; }

		/// <summary>
		/// The name of the queue the handler consumes.
		/// </summary>
		public string QueueName { get; }

		/// <summary>
		/// The binding key between <see cref="ExchangeName"/> and <see cref="QueueName"/>.
		/// </summary>
		public string RoutingKey { get; }

		/// <summary>
		/// The consumer tag registered with the broker, <c>{exchange}.{queue}.{routingKey}</c>, which
		/// identifies this consumer in logs and the management UI.
		/// </summary>
		public string ConsumerTag { get; }
	}
}
