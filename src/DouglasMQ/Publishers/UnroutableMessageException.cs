using System;

namespace DouglasMQ
{
	/// <summary>
	/// Thrown by <see cref="IPublisher.PublishAsync{TEvent}"/> and <see cref="IPublisher.SendAsync{TCommand}"/>
	/// when the broker cannot route a message: its exchange does not exist, or no queue is bound to the
	/// exchange for its routing key. The message has not been delivered anywhere.
	/// </summary>
	/// <remarks>
	/// A missing exchange is always reported this way. A message the broker returns as unroutable
	/// (<c>basic.return</c>) is reported this way only when <see cref="Configuration.PublisherConfirms"/>
	/// is on, because without confirms the publish has already returned when the broker answers; the
	/// publisher then logs the return at Error instead.
	/// </remarks>
	public class UnroutableMessageException : Exception
	{
		/// <summary>The AMQP reply code the broker sends for an unroutable message (<c>NO_ROUTE</c>).</summary>
		public const ushort NoRoute = 312;

		/// <summary>The AMQP reply code the broker sends when the exchange does not exist (<c>NOT_FOUND</c>).</summary>
		public const ushort NotFound = 404;

		/// <summary>Initialises a new <see cref="UnroutableMessageException"/>.</summary>
		public UnroutableMessageException()
		{
		}

		/// <summary>Initialises a new <see cref="UnroutableMessageException"/> with a message.</summary>
		/// <param name="message">Describes the failure.</param>
		public UnroutableMessageException(string message)
			: base(message)
		{
		}

		/// <summary>Initialises a new <see cref="UnroutableMessageException"/> with a message and the failure that caused it.</summary>
		/// <param name="message">Describes the failure.</param>
		/// <param name="innerException">The underlying failure.</param>
		public UnroutableMessageException(string message, Exception innerException)
			: base(message, innerException)
		{
		}

		/// <summary>Initialises a new <see cref="UnroutableMessageException"/> for a message the broker could not route.</summary>
		/// <param name="exchange">The exchange the message was published to.</param>
		/// <param name="routingKey">The routing key it was published with.</param>
		/// <param name="replyCode">The broker's reply code: <see cref="NoRoute"/> or <see cref="NotFound"/>.</param>
		/// <param name="replyText">The broker's reply text.</param>
		/// <param name="innerException">The RabbitMQ.Client exception that reported it, if any.</param>
		public UnroutableMessageException(string exchange, string routingKey, ushort replyCode, string replyText, Exception? innerException = null)
			: base(Describe(exchange, routingKey, replyCode, replyText), innerException)
		{
			Exchange = exchange;
			RoutingKey = routingKey;
			ReplyCode = replyCode;
			ReplyText = replyText;
		}

		/// <summary>The exchange the message was published to.</summary>
		public string Exchange { get; } = string.Empty;

		/// <summary>The routing key the message was published with.</summary>
		public string RoutingKey { get; } = string.Empty;

		/// <summary>The broker's reply code: <see cref="NoRoute"/> (no queue bound) or <see cref="NotFound"/> (no such exchange).</summary>
		public ushort ReplyCode { get; }

		/// <summary>The broker's reply text, for example <c>NO_ROUTE</c>.</summary>
		public string ReplyText { get; } = string.Empty;

		private static string Describe(string exchange, string routingKey, ushort replyCode, string replyText)
			=> replyCode == NotFound
				? $"The message could not be published: exchange '{exchange}' does not exist ({replyCode} {replyText}). Start a consumer that declares it, or declare it yourself."
				: $"The message could not be routed: no queue is bound to exchange '{exchange}' for routing key '{routingKey}' ({replyCode} {replyText}).";
	}
}
