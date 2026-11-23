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
	/// <para>
	/// Every delivery ends in exactly one outcome: ack (handled), nack without requeue
	/// (dead-lettered), or — only while the consumer is shutting down — left unacknowledged so
	/// the broker redelivers it once the channel closes.
	/// </para>
	/// <para>
	/// Stopping is graceful and happens once, however many <see cref="StopAsync"/> and
	/// <see cref="DisposeAsync"/> calls overlap: the subscription is cancelled so no new deliveries
	/// arrive, handlers already running finish and settle, and only then is the channel closed.
	/// </para>
	/// </remarks>
	internal sealed class Consumer : IAsyncDisposable
	{
		/// <summary>
		/// How long <see cref="DisposeAsync"/> waits for in-flight handlers when <see cref="StopAsync"/>
		/// has not been called with a token of its own.
		/// </summary>
		internal static readonly TimeSpan DefaultDrainTimeout = TimeSpan.FromSeconds(30);

		private readonly IConnection _connection;
		private readonly HandlerRegistration _registration;
		private readonly DeliveryProcessor _processor;
		private readonly ushort _prefetchCount;
		private readonly ILogger _logger;

		// Lifetime token for message processing. Deliberately separate from the StartAsync token,
		// which only governs startup and may be short-lived or already cancelled afterwards. Never
		// disposed: a late delivery may still read its token, and it holds no timer.
		private readonly CancellationTokenSource _stopping = new();

		// Guards _inFlight, _stopRequested and _stopTask.
		private readonly object _gate = new();
		private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
		private int _inFlight;
		private bool _stopRequested;
		private Task? _stopTask;
		private IChannel? _channel;
		private string? _consumerTag;

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

			var processor = new DeliveryProcessor(serializer, registration.Invoker, RetryPolicy.From(configuration), logger, registration.Binding);
			return new Consumer(connection, registration, processor, configuration.PrefetchCount, logger);
		}

		/// <summary>
		/// Declares topology, sets QoS, and begins consuming messages.
		/// </summary>
		/// <param name="cancellationToken">Cancels startup only; it does not affect message processing afterwards.</param>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			lock (_gate)
			{
				ObjectDisposedException.ThrowIf(_stopRequested, this);
			}

			var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
			_channel = channel;
			await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);
			await Topology.DeclareConsumerQueueAsync(channel, _registration.Binding, cancellationToken);

			var consumer = new AsyncEventingBasicConsumer(channel);
			consumer.ReceivedAsync += (_, eventArgs) => HandleDeliveryAsync(channel, eventArgs);

			_consumerTag = await channel.BasicConsumeAsync(
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
			lock (_gate)
			{
				if (_stopRequested)
				{
					// Arrived (or was still queued in the client) after stopping began. Leave it
					// unacknowledged: the broker requeues it when the channel closes.
					return;
				}

				_inFlight++;
			}

			try
			{
				var delivery = new DeliveryInfo(eventArgs.Exchange, eventArgs.RoutingKey, eventArgs.BasicProperties?.Headers);
				var outcome = await _processor.ProcessAsync(eventArgs.Body, delivery, _stopping.Token);
				if (outcome != DeliveryOutcome.Abandon)
				{
					await SettleAsync(channel, eventArgs.DeliveryTag, outcome);
				}
			}
			finally
			{
				lock (_gate)
				{
					_inFlight--;
					if (_stopRequested && _inFlight == 0)
					{
						_drained.TrySetResult();
					}
				}
			}
		}

		private async Task SettleAsync(IChannel channel, ulong deliveryTag, DeliveryOutcome outcome)
		{
			try
			{
				// CancellationToken.None: an ack/nack must not be skipped because of cancellation,
				// otherwise the message is stuck unacknowledged and holds a prefetch slot.
				if (outcome == DeliveryOutcome.Ack)
				{
					await channel.BasicAckAsync(deliveryTag, multiple: false, CancellationToken.None);
				}
				else
				{
					await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, CancellationToken.None);
				}
			}
			catch (Exception ex) when (!channel.IsOpen)
			{
				// Channel went away between handling and settling. Nothing else can be done here;
				// the broker will redeliver the message.
				_logger.LogWarning(ex, "Could not settle delivery {DeliveryTag} on {ConsumerTag}: channel is closed. It will be redelivered.",
					deliveryTag, ConsumerTag);
			}
		}

		/// <summary>
		/// Stops the consumer gracefully: cancels the broker subscription, waits for in-flight handlers to
		/// finish and settle, then closes the channel. Deliveries not yet started are left for redelivery.
		/// </summary>
		/// <param name="cancellationToken">
		/// Bounds the wait for in-flight handlers (typically the host's shutdown timeout). When it fires, the
		/// channel is closed anyway and unsettled deliveries are redelivered.
		/// </param>
		/// <remarks>
		/// Stopping happens once. Every call, overlapping or later, waits for that one stop to finish;
		/// only the first call's token applies.
		/// </remarks>
		public Task StopAsync(CancellationToken cancellationToken)
		{
			lock (_gate)
			{
				if (_stopTask is null)
				{
					_stopRequested = true;
					if (_inFlight == 0)
					{
						_drained.TrySetResult();
					}

					// Task.Run: run the stop outside the lock, which in-flight deliveries also take.
					_stopTask = Task.Run(() => StopCoreAsync(cancellationToken), CancellationToken.None);
				}

				return _stopTask;
			}
		}

		private async Task StopCoreAsync(CancellationToken cancellationToken)
		{
			var channel = _channel;

			// 1. No new deliveries.
			if (channel is { IsOpen: true } && _consumerTag is not null)
			{
				try
				{
					await channel.BasicCancelAsync(_consumerTag, noWait: false, cancellationToken);
				}
				catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
				{
					_logger.LogDebug(ex, "Could not cancel subscription {ConsumerTag}; closing the channel will end it.", ConsumerTag);
				}
				catch (OperationCanceledException)
				{
					// Out of time: fall through and close the channel.
				}
			}

			// 2. End retry delays early (those deliveries are redelivered), then let running handlers
			// finish and settle, bounded by the caller's token.
			await _stopping.CancelAsync();
			try
			{
				await _drained.Task.WaitAsync(cancellationToken);
			}
			catch (OperationCanceledException)
			{
				int inFlight;
				lock (_gate)
				{
					inFlight = _inFlight;
				}

				_logger.LogWarning("Consumer {ConsumerTag} stopped before {InFlight} in-flight delivery(ies) finished; they will be redelivered.",
					ConsumerTag, inFlight);
			}

			// 3. Only then close the channel.
			await AmqpResources.CloseAndDisposeAsync(channel);
		}

		/// <summary>
		/// Stops the consumer (see <see cref="StopAsync"/>), waiting up to <see cref="DefaultDrainTimeout"/>
		/// for in-flight handlers unless a stop with its own token is already under way.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			using var timeout = new CancellationTokenSource(DefaultDrainTimeout);
			await StopAsync(timeout.Token);
		}
	}
}
