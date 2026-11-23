using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DouglasMQ
{
	/// <summary>
	/// Manages a single RabbitMQ consumer: declares topology, applies QoS, and dispatches
	/// received messages to the registered handler with retry and dead-letter support.
	/// </summary>
	/// <remarks>
	/// Every delivery ends in exactly one outcome: ack (handled), nack without requeue
	/// (dead-lettered), or — only while the consumer is shutting down — left unacknowledged so
	/// the broker redelivers it once the channel closes.
	/// </remarks>
	internal sealed class Consumer : IAsyncDisposable
	{
		private readonly IConnection _connection;
		private readonly ISerializer _serializer;
		private readonly ILogger _logger;
		private readonly HandlerAttribute _attribute;
		private readonly HandlerInvoker _invoker;
		private readonly int _retryCount;
		private readonly int _retryDelayMs;
		private readonly ushort _prefetchCount;

		// Lifetime token for message processing. Deliberately separate from the StartAsync token,
		// which only governs startup and may be short-lived or already cancelled afterwards.
		private readonly CancellationTokenSource _stopping = new();
		private IChannel? _channel;
		private bool _disposed;

		private Consumer(
			IConnection connection,
			ISerializer serializer,
			ILogger logger,
			Configuration configuration,
			HandlerAttribute attribute,
			HandlerInvoker invoker)
		{
			_connection = connection;
			_serializer = serializer;
			_logger = logger;
			_attribute = attribute;
			_invoker = invoker;
			_retryCount = Math.Max(0, configuration.RetryCount);
			_retryDelayMs = Math.Max(0, configuration.RetryDelayMs);
			_prefetchCount = configuration.PrefetchCount;
		}

		/// <summary>The queue this consumer reads from.</summary>
		internal string QueueName => _attribute.QueueName;

		/// <summary>The handler class this consumer dispatches to.</summary>
		internal string HandlerName => _invoker.HandlerName;

		/// <summary>
		/// Creates a new <see cref="Consumer"/> from a <see cref="HandlerAttribute"/> and its handler instance.
		/// </summary>
		/// <param name="connection">The connection used to open the consumer's channel.</param>
		/// <param name="serializer">Deserializes message bodies.</param>
		/// <param name="logger">Logger for operational events.</param>
		/// <param name="configuration">Supplies QoS and retry settings.</param>
		/// <param name="attribute">Topology (exchange, queue, routing key) for this consumer.</param>
		/// <param name="handler">The handler instance that processes messages.</param>
		/// <param name="messageType">
		/// The message type to dispatch to. Required when the handler implements more than one
		/// <see cref="IHandler{TMessage}"/>; inferred otherwise.
		/// </param>
		/// <exception cref="InvalidOperationException">The handler's message type cannot be determined.</exception>
		public static Consumer Create(
			IConnection connection,
			ISerializer serializer,
			ILogger logger,
			Configuration configuration,
			HandlerAttribute attribute,
			object handler,
			Type? messageType = null)
			=> Create(connection, serializer, logger, configuration, attribute, HandlerInvoker.Create(handler, messageType));

		/// <summary>
		/// Creates a new <see cref="Consumer"/> that dispatches through <paramref name="invoker"/>, for
		/// example one that resolves the handler in a new dependency injection scope per message.
		/// </summary>
		public static Consumer Create(
			IConnection connection,
			ISerializer serializer,
			ILogger logger,
			Configuration configuration,
			HandlerAttribute attribute,
			HandlerInvoker invoker)
			=> new Consumer(connection, serializer, logger, configuration, attribute, invoker);

		/// <summary>
		/// Declares topology, sets QoS, and begins consuming messages.
		/// </summary>
		/// <param name="cancellationToken">Cancels startup only; it does not affect message processing afterwards.</param>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			ObjectDisposedException.ThrowIf(_disposed, this);

			_channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
			await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);
			await DeclareRemoteAsync(_channel, cancellationToken);

			var channel = _channel;
			var consumer = new AsyncEventingBasicConsumer(channel);
			consumer.ReceivedAsync += (_, eventArgs) => HandleDeliveryAsync(channel, eventArgs);

			await _channel.BasicConsumeAsync(
				queue: _attribute.QueueName,
				autoAck: false,
				consumerTag: _attribute.ConsumerTag,
				noLocal: false,
				exclusive: false,
				arguments: null,
				consumer: consumer,
				cancellationToken: cancellationToken);
		}

		private async Task DeclareRemoteAsync(IChannel channel, CancellationToken cancellationToken)
		{
			await channel.ExchangeDeclareAsync(_attribute.ExchangeName, _attribute.ExchangeType, durable: true, autoDelete: false, cancellationToken: cancellationToken);
			await channel.QueueDeclareAsync(_attribute.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>()
			{
				{ "x-dead-letter-exchange", "dlx.exchange" },
				{ "x-dead-letter-routing-key", "dlx_routing_key" }
			}, cancellationToken: cancellationToken);
			await channel.QueueBindAsync(_attribute.QueueName, _attribute.ExchangeName, _attribute.RoutingKey, cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Handles a single delivery. Internal (rather than private) so the ack/nack contract can be
		/// unit tested without a broker.
		/// </summary>
		internal async Task HandleDeliveryAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
		{
			var stoppingToken = _stopping.Token;

			using var activity = StartProcessActivity(eventArgs);
			var telemetry = new DeliveryTelemetry(activity);
			var startTimestamp = Stopwatch.GetTimestamp();

			try
			{
				bool handled;
				try
				{
					handled = await TryHandleAsync(eventArgs, telemetry, stoppingToken);
				}
				catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
				{
					// Shutting down mid-processing: leave the delivery unacknowledged. The broker
					// requeues it when the channel closes, so another consumer can pick it up.
					_logger.LogInformation("Consumer {ConsumerTag} stopping; delivery {DeliveryTag} will be redelivered.",
						_attribute.ConsumerTag, eventArgs.DeliveryTag);
					telemetry.Outcome = MessagingTags.OutcomeRequeued;
					return;
				}

				telemetry.Outcome = handled ? MessagingTags.OutcomeAck : MessagingTags.OutcomeDeadLettered;

				try
				{
					// CancellationToken.None: an ack/nack must not be skipped because of cancellation,
					// otherwise the message is stuck unacknowledged and holds a prefetch slot.
					if (handled)
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
						eventArgs.DeliveryTag, _attribute.ConsumerTag);
					telemetry.Outcome = MessagingTags.OutcomeRequeued;
					activity?.AddException(ex);
				}
			}
			finally
			{
				RecordDelivery(telemetry, Stopwatch.GetElapsedTime(startTimestamp));
			}
		}

		/// <summary>
		/// Mutable per-delivery state collected while handling, then written to the span and metrics once.
		/// </summary>
		private sealed class DeliveryTelemetry
		{
			public DeliveryTelemetry(Activity? activity) => Activity = activity;

			public Activity? Activity { get; }

			public string? Outcome { get; set; }

			public string? ErrorType { get; set; }

			public int Retries { get; set; }
		}

		private void RecordDelivery(DeliveryTelemetry telemetry, TimeSpan elapsed)
		{
			var activity = telemetry.Activity;
			if (activity is not null)
			{
				activity.SetTag(MessagingTags.RetryCount, telemetry.Retries);
				activity.SetTag(MessagingTags.Outcome, telemetry.Outcome);
				if (telemetry.ErrorType is not null)
				{
					activity.SetTag(MessagingTags.ErrorType, telemetry.ErrorType);
				}

				if (telemetry.Outcome == MessagingTags.OutcomeDeadLettered)
				{
					activity.SetStatus(ActivityStatusCode.Error, $"Message dead-lettered ({telemetry.ErrorType}).");
				}
			}

			var tags = new TagList
			{
				{ MessagingTags.System, MessagingTags.SystemValue },
				{ MessagingTags.DestinationName, _attribute.ExchangeName },
				{ MessagingTags.SubscriptionName, _attribute.QueueName },
				{ MessagingTags.Handler, _invoker.HandlerName },
				{ MessagingTags.MessageType, _invoker.MessageType.Name },
			};

			if (telemetry.Outcome is not null)
			{
				tags.Add(MessagingTags.Outcome, telemetry.Outcome);
			}

			if (telemetry.ErrorType is not null)
			{
				tags.Add(MessagingTags.ErrorType, telemetry.ErrorType);
			}

			DouglasMQTelemetry.MessagesConsumed.Add(1, tags);
			if (telemetry.Outcome == MessagingTags.OutcomeDeadLettered)
			{
				DouglasMQTelemetry.MessagesDeadLettered.Add(1, tags);
			}

			DouglasMQTelemetry.ProcessDuration.Record(elapsed.TotalSeconds, tags);
		}

		private Activity? StartProcessActivity(BasicDeliverEventArgs eventArgs)
		{
			if (!DouglasMQTelemetry.ActivitySource.HasListeners())
			{
				return null;
			}

			var headers = eventArgs.BasicProperties?.Headers;
			var remoteContext = DouglasMQTelemetry.ExtractContext(headers);

			// RabbitMQ.Client's own "deliver" activity may already be current and parented on the
			// same remote context. Nest under it in that case; otherwise parent on the publisher's context.
			var current = Activity.Current;
			var parent = current is not null && (remoteContext == default || current.TraceId == remoteContext.TraceId)
				? current.Context
				: remoteContext;

			var activity = DouglasMQTelemetry.ActivitySource.StartActivity(
				$"process {_attribute.QueueName}",
				ActivityKind.Consumer,
				parent);

			if (activity is null)
			{
				return null;
			}

			foreach (var baggage in DouglasMQTelemetry.ExtractBaggage(headers))
			{
				if (activity.GetBaggageItem(baggage.Key) is null)
				{
					activity.SetBaggage(baggage.Key, baggage.Value);
				}
			}

			if (activity.IsAllDataRequested)
			{
				activity.SetTag(MessagingTags.System, MessagingTags.SystemValue);
				activity.SetTag(MessagingTags.OperationName, "process");
				activity.SetTag(MessagingTags.OperationType, "process");
				activity.SetTag(MessagingTags.DestinationName, _attribute.ExchangeName);
				activity.SetTag(MessagingTags.SubscriptionName, _attribute.QueueName);
				activity.SetTag(MessagingTags.RoutingKey, eventArgs.RoutingKey);
				activity.SetTag(MessagingTags.DeliveryTag, (long)eventArgs.DeliveryTag);
				activity.SetTag(MessagingTags.Redelivered, eventArgs.Redelivered);
				activity.SetTag(MessagingTags.BodySize, eventArgs.Body.Length);
				activity.SetTag(MessagingTags.Handler, _invoker.HandlerName);
				activity.SetTag(MessagingTags.MessageType, _invoker.MessageType.Name);
			}

			return activity;
		}

		/// <returns><c>true</c> to ack; <c>false</c> to dead-letter.</returns>
		private async Task<bool> TryHandleAsync(BasicDeliverEventArgs eventArgs, DeliveryTelemetry telemetry, CancellationToken stoppingToken)
		{
			object? message;
			try
			{
				var payload = Encoding.UTF8.GetString(eventArgs.Body.Span);
				message = _serializer.Deserialize(payload, _invoker.MessageType);
			}
			catch (Exception ex)
			{
				// Poison message: retrying cannot help, so dead-letter immediately.
				_logger.LogError(ex, "Failed to deserialize message for {MessageType} on {ConsumerTag}. Dead-lettering.",
					_invoker.MessageType.Name, _attribute.ConsumerTag);
				telemetry.ErrorType = ex.GetType().FullName;
				telemetry.Activity?.AddException(ex);
				return false;
			}

			if (message is null)
			{
				_logger.LogWarning("Received null message for {MessageType} on {ConsumerTag}. Dead-lettering.",
					_invoker.MessageType.Name, _attribute.ConsumerTag);
				telemetry.ErrorType = "deserialization_failed";
				return false;
			}

			if (message is IMessage typedMessage && telemetry.Activity is { IsAllDataRequested: true } activity)
			{
				activity.SetTag(MessagingTags.MessageId, typedMessage.MessageId.ToString());
				activity.SetTag(MessagingTags.ConversationId, typedMessage.TraceId.ToString());
			}

			var lastException = default(Exception);
			for (var attempt = 0; attempt <= _retryCount; attempt++)
			{
				if (attempt > 0)
				{
					var delay = GetRetryDelay(attempt);
					_logger.LogWarning("Retrying handler {Handler} for message {MessageType}, attempt {Attempt}/{RetryCount} after {DelayMs}ms.",
						_invoker.HandlerName, _invoker.MessageType.Name, attempt, _retryCount, delay.TotalMilliseconds);

					telemetry.Retries = attempt;
					DouglasMQTelemetry.HandlerRetries.Add(1, new TagList
					{
						{ MessagingTags.System, MessagingTags.SystemValue },
						{ MessagingTags.DestinationName, _attribute.ExchangeName },
						{ MessagingTags.SubscriptionName, _attribute.QueueName },
						{ MessagingTags.Handler, _invoker.HandlerName },
						{ MessagingTags.MessageType, _invoker.MessageType.Name },
					});
					telemetry.Activity?.AddEvent(new ActivityEvent("douglasmq.retry", tags: new ActivityTagsCollection
					{
						{ MessagingTags.Attempt, attempt },
						{ "douglasmq.retry.delay_ms", delay.TotalMilliseconds },
					}));

					// Throws OperationCanceledException on shutdown, handled by the caller.
					await Task.Delay(delay, stoppingToken);
				}

				try
				{
					await _invoker.InvokeAsync(message);
					return true;
				}
				catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
				{
					throw;
				}
				catch (Exception ex)
				{
					// Includes OperationCanceledException raised by the handler itself (e.g. an
					// HttpClient timeout), which is a handler failure, not a consumer shutdown.
					lastException = ex;

					// Record every failed attempt so the trace shows the full retry history.
					telemetry.Activity?.AddException(ex, new TagList { { MessagingTags.Attempt, attempt } });
				}
			}

			_logger.LogError(lastException,
				"Handler {Handler} failed for message {MessageType} after {Attempts} attempt(s). Dead-lettering.",
				_invoker.HandlerName, _invoker.MessageType.Name, _retryCount + 1);
			telemetry.ErrorType = lastException?.GetType().FullName;
			return false;
		}

		/// <summary>
		/// Exponential backoff: base delay × 2^(attempt-1), capped so large retry counts cannot
		/// overflow or produce an invalid <see cref="Task.Delay(TimeSpan)"/> argument.
		/// </summary>
		internal TimeSpan GetRetryDelay(int attempt)
		{
			var delayMs = _retryDelayMs * Math.Pow(2, attempt - 1);
			return TimeSpan.FromMilliseconds(Math.Min(delayMs, MaxRetryDelay.TotalMilliseconds));
		}

		private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(5);

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

			var channel = _channel;
			if (channel is not null)
			{
				try
				{
					if (channel.IsOpen)
					{
						await channel.CloseAsync();
					}
				}
				finally
				{
					await channel.DisposeAsync();
				}
			}

			_stopping.Dispose();
		}
	}
}
