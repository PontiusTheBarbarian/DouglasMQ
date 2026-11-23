using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Owns every exchange, queue and binding DouglasMQ declares on the broker, so the names and
	/// flags those declarations share (the dead-letter wiring, durability) are defined once.
	/// </summary>
	internal static class Topology
	{
		/// <summary>Exchange that receives messages rejected by any DouglasMQ queue.</summary>
		public const string DeadLetterExchange = "dlx.exchange";

		/// <summary>Queue that collects dead-lettered messages.</summary>
		public const string DeadLetterQueue = "dlx.queue";

		/// <summary>Routing key that binds <see cref="DeadLetterQueue"/> to <see cref="DeadLetterExchange"/>.</summary>
		public const string DeadLetterRoutingKey = "dlx_routing_key";

		/// <summary>
		/// Converts an <see cref="ExchangeType"/> to the name the AMQP protocol expects (e.g. <c>"topic"</c>).
		/// </summary>
		public static string ToAmqpName(ExchangeType exchangeType) => exchangeType switch
		{
			ExchangeType.direct => RabbitMQ.Client.ExchangeType.Direct,
			ExchangeType.topic => RabbitMQ.Client.ExchangeType.Topic,
			ExchangeType.fanout => RabbitMQ.Client.ExchangeType.Fanout,
			ExchangeType.headers => RabbitMQ.Client.ExchangeType.Headers,
			_ => throw new System.ArgumentOutOfRangeException(nameof(exchangeType), exchangeType, "Unknown exchange type."),
		};

		/// <summary>
		/// Declares the dead-letter exchange and queue that every consumer queue forwards rejected messages to.
		/// </summary>
		public static async Task DeclareDeadLetterAsync(IChannel channel, CancellationToken cancellationToken)
		{
			await channel.ExchangeDeclareAsync(DeadLetterExchange, RabbitMQ.Client.ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: cancellationToken);
			await channel.QueueDeclareAsync(DeadLetterQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
			await channel.QueueBindAsync(DeadLetterQueue, DeadLetterExchange, DeadLetterRoutingKey, cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Declares the exchange and durable queue described by <paramref name="binding"/>, binds them, and
		/// points the queue's dead-lettering at <see cref="DeadLetterExchange"/>.
		/// </summary>
		public static async Task DeclareConsumerQueueAsync(IChannel channel, HandlerAttribute binding, CancellationToken cancellationToken)
		{
			await channel.ExchangeDeclareAsync(binding.ExchangeName, binding.ExchangeType, durable: true, autoDelete: false, cancellationToken: cancellationToken);
			await channel.QueueDeclareAsync(binding.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
			{
				{ "x-dead-letter-exchange", DeadLetterExchange },
				{ "x-dead-letter-routing-key", DeadLetterRoutingKey },
			}, cancellationToken: cancellationToken);
			await channel.QueueBindAsync(binding.QueueName, binding.ExchangeName, binding.RoutingKey, cancellationToken: cancellationToken);
		}
	}
}
