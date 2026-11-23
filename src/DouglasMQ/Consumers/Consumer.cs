using System;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DouglasMQ
{
	/// <summary>
	/// Connects one <see cref="HandlerRegistration"/> to RabbitMQ: opens a channel, declares the queue,
	/// applies QoS, and settles each delivery according to the <see cref="DeliveryProcessor"/>'s outcome.
	/// </summary>
	/// <remarks>
	/// Every delivery ends in exactly one outcome: ack (handled), nack without requeue
	/// (dead-lettered), or — only while the consumer is shutting down — left unacknowledged so
	/// the broker redelivers it once the channel closes.
	/// </remarks>
	internal sealed class Consumer : IAsyncDisposable
	{
		private readonly IConnection _connection;
		private readonly HandlerRegistration _registration;
		private readonly DeliveryProcessor _processor;
		private readonly ushort _prefetchCount;
		private readonly ILogger _logger;

		// Lifetime token for message processing. Deliberately separate from the StartAsync token,
		// which only governs startup and may be short-lived or already cancelled afterwards.
		private readonly CancellationTokenSource _stopping = new();
		private IChannel? _channel;
		private bool _disposed;

		private Consumer(IConnection connection, HandlerRegistration registration, DeliveryProcessor processor, ushort prefetchCount, ILogger logger)
		{
			_connection = connection;
			_registration = registration;
			_processor = processor;
			_prefetchCount = prefetchCount;
			_logger = logger;
		}

		/// <summary>The queue this consumer reads from.</summary>
		internal string QueueName => _registration.Binding.QueueName;

		/// <summary>The handler class this consumer dispatches to.</summary>
		internal string HandlerName => _registration.Invoker.HandlerName;

		private string ConsumerTag => _registration.Binding.ConsumerTag;

		/// <summary>
		/// Creates a consumer for <paramref name="registration"/> on <paramref name="connection"/>.
		/// </summary>
		/// <param name="connection">The connection used to open the consumer's channel.</param>
		/// <param name="registration">The queue to consume and the handler to dispatch to.</param>
		/// <param name="serializer">Deserializes message bodies.</param>
		/// <param name="configuration">Supplies QoS and retry settings.</param>
		/// <param name="logger">Logger for operational events.</param>
		public static Consumer Create(IConnection connection, HandlerRegistration registration, ISerializer serializer, Configuration configuration, ILogger logger)
		{
			ArgumentNullException.ThrowIfNull(connection);
			ArgumentNullException.ThrowIfNull(registration);
			ArgumentNullException.ThrowIfNull(configuration);

			var processor = new DeliveryProcessor(serializer, registration.Invoker, RetryPolicy.From(configuration), logger, registration.Binding.ConsumerTag);
			return new Consumer(connection, registration, processor, configuration.PrefetchCount, logger);
		}

		/// <summary>
		/// Declares topology, sets QoS, and begins consuming messages.
		/// </summary>
		/// <param name="cancellationToken">Cancels startup only; it does not affect message processing afterwards.</param>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			ObjectDisposedException.ThrowIf(_disposed, this);

			var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
			_channel = channel;
			await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);
			await Topology.DeclareConsumerQueueAsync(channel, _registration.Binding, cancellationToken);

			var consumer = new AsyncEventingBasicConsumer(channel);
			consumer.ReceivedAsync += (_, eventArgs) => HandleDeliveryAsync(channel, eventArgs);

			await channel.BasicConsumeAsync(
				queue: QueueName,
				autoAck: false,
				consumerTag: ConsumerTag,
				noLocal: false,
				exclusive: false,
				arguments: null,
				consumer: consumer,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Processes a single delivery and settles it. Internal (rather than private) so the ack/nack
		/// contract can be unit tested without a broker.
		/// </summary>
		internal async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
		{
			var outcome = await _processor.ProcessAsync(eventArgs.Body, _stopping.Token);
			if (outcome == DeliveryOutcome.Abandon)
			{
				// The broker requeues the unacknowledged delivery when the channel closes, so another
				// consumer can pick it up.
				return;
			}

			try
			{
				// CancellationToken.None: an ack/nack must not be skipped because of cancellation,
				// otherwise the message is stuck unacknowledged and holds a prefetch slot.
				if (outcome == DeliveryOutcome.Ack)
				{
					await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, CancellationToken.None);
				}
				else
				{
					await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, CancellationToken.None);
				}
			}
			catch (Exception ex) when (!channel.IsOpen)
			{
				// Channel went away between handling and settling. Nothing else can be done here;
				// the broker will redeliver the message.
				_logger.LogWarning(ex, "Could not settle delivery {DeliveryTag} on {ConsumerTag}: channel is closed. It will be redelivered.",
					eventArgs.DeliveryTag, ConsumerTag);
			}
		}

		/// <summary>
		/// Stops message processing and closes the consumer's channel.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			if (_disposed)
			{
				return;
			}

			_disposed = true;
			await _stopping.CancelAsync();

			try
			{
				await AmqpResources.CloseAndDisposeAsync(_channel);
			}
			finally
			{
				_stopping.Dispose();
			}
		}
	}
}
