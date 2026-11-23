using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;

namespace DouglasMQ
{
	/// <summary>
	/// Turns a message body into a <see cref="DeliveryOutcome"/>: deserializes it, runs the handler, and
	/// retries with backoff on failure. Each attempt runs inside its own trace span and logger scope.
	/// Knows nothing about channels or acknowledgements, so the processing rules can be tested without RabbitMQ.
	/// </summary>
	internal sealed class DeliveryProcessor
	{
		private readonly ISerializer _serializer;
		private readonly HandlerInvoker _invoker;
		private readonly RetryPolicy _retryPolicy;
		private readonly ILogger _logger;
		private readonly HandlerAttribute _binding;
		private readonly string _activityName;

		/// <param name="serializer">Deserializes message bodies.</param>
		/// <param name="invoker">Calls the handler.</param>
		/// <param name="retryPolicy">How often and how long apart to retry a failing handler.</param>
		/// <param name="logger">Logger for processing failures; also carries the per-message log scope.</param>
		/// <param name="binding">The queue being consumed, used in logs and traces.</param>
		public DeliveryProcessor(ISerializer serializer, HandlerInvoker invoker, RetryPolicy retryPolicy, ILogger logger, HandlerAttribute binding)
		{
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			_invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
			_retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_binding = binding ?? throw new ArgumentNullException(nameof(binding));
			_activityName = $"{binding.QueueName} process";
		}

		private string ConsumerTag => _binding.ConsumerTag;

		/// <summary>
		/// Processes one message body.
		/// </summary>
		/// <param name="body">The raw message body.</param>
		/// <param name="delivery">Where the message came from, and its headers (for trace context).</param>
		/// <param name="stoppingToken">
		/// Signals consumer shutdown. Cancelling it during a retry delay yields <see cref="DeliveryOutcome.Abandon"/>.
		/// A handler's own <see cref="OperationCanceledException"/> (e.g. an HTTP timeout) while this token
		/// is not cancelled counts as an ordinary failure.
		/// </param>
		public async Task<DeliveryOutcome> ProcessAsync(ReadOnlyMemory<byte> body, DeliveryInfo delivery, CancellationToken stoppingToken)
		{
			object? message;
			try
			{
				message = _serializer.DeserializeFromBytes(body.Span, _invoker.MessageType);
			}
			catch (Exception ex)
			{
				// Poison message: retrying cannot help, so dead-letter immediately.
				_logger.LogError(ex, "Failed to deserialize message for {MessageType} on {ConsumerTag}. Dead-lettering.",
					_invoker.MessageType.Name, ConsumerTag);
				return DeliveryOutcome.DeadLetter;
			}

			if (message is null)
			{
				_logger.LogWarning("Received null message for {MessageType} on {ConsumerTag}. Dead-lettering.",
					_invoker.MessageType.Name, ConsumerTag);
				return DeliveryOutcome.DeadLetter;
			}

			// IHandler<T> constrains T to IMessage, so this always succeeds for real handlers.
			var identity = message as IMessage;
			var parent = ResolveParentContext(delivery, identity);

			var lastException = default(Exception);
			for (var attempt = 0; attempt < _retryPolicy.MaxAttempts; attempt++)
			{
				if (attempt > 0)
				{
					var delay = _retryPolicy.GetDelay(attempt);
					_logger.LogWarning("Retrying handler {Handler} for message {MessageType}, attempt {Attempt}/{RetryCount} after {DelayMs}ms.",
						_invoker.HandlerName, _invoker.MessageType.Name, attempt, _retryPolicy.RetryCount, delay.TotalMilliseconds);

					try
					{
						await Task.Delay(delay, stoppingToken);
					}
					catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
					{
						return Abandon();
					}
				}

				using var activity = StartAttemptActivity(parent, delivery, identity, attempt + 1);
				using var logScope = BeginMessageScope(identity);
				try
				{
					await _invoker.InvokeAsync(message);
					return DeliveryOutcome.Ack;
				}
				catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
				{
					return Abandon();
				}
				catch (Exception ex)
				{
					RecordFailure(activity, ex);

					if (NonRetryableException.Find(ex) is { } nonRetryable)
					{
						_logger.LogWarning(ex,
							"Handler {Handler} rejected message {MessageType} as non-retryable: {Reason} Dead-lettering without retrying.",
							_invoker.HandlerName, _invoker.MessageType.Name, nonRetryable.Message);
						return DeliveryOutcome.DeadLetter;
					}

					// Includes OperationCanceledException raised by the handler itself (e.g. an
					// HttpClient timeout), which is a handler failure, not a consumer shutdown.
					lastException = ex;
				}
			}

			_logger.LogError(lastException,
				"Handler {Handler} failed for message {MessageType} after {Attempts} attempt(s). Dead-lettering.",
				_invoker.HandlerName, _invoker.MessageType.Name, _retryPolicy.MaxAttempts);
			return DeliveryOutcome.DeadLetter;
		}

		private DeliveryOutcome Abandon()
		{
			_logger.LogInformation("Consumer {ConsumerTag} stopping; delivery will be redelivered.", ConsumerTag);
			return DeliveryOutcome.Abandon;
		}

		/// <summary>
		/// The sender's trace context: the <c>traceparent</c> header when the publisher set one, otherwise
		/// the trace named by the message's <c>TraceId</c>. An already-current activity in the same trace
		/// (such as RabbitMQ.Client's own receive span) is preferred, so DouglasMQ's span nests under it.
		/// </summary>
		private static ActivityContext ResolveParentContext(DeliveryInfo delivery, IMessage? identity)
		{
			var remote = DouglasMQTelemetry.ExtractContext(delivery.Headers);
			if (remote == default && identity is not null)
			{
				remote = DouglasMQTelemetry.FromMessageIds(identity.TraceId, identity.MessageId);
			}

			var current = Activity.Current;
			if (current is not null && (remote == default || current.TraceId == remote.TraceId))
			{
				return current.Context;
			}

			return remote;
		}

		private Activity? StartAttemptActivity(ActivityContext parent, DeliveryInfo delivery, IMessage? identity, int attempt)
		{
			var activity = DouglasMQTelemetry.ActivitySource.StartActivity(_activityName, ActivityKind.Consumer, parent);
			if (activity is { IsAllDataRequested: true })
			{
				activity.SetTag(MessagingTags.System, MessagingTags.SystemValue);
				activity.SetTag(MessagingTags.OperationType, "process");
				activity.SetTag(MessagingTags.DestinationName, string.IsNullOrEmpty(delivery.Exchange) ? _binding.ExchangeName : delivery.Exchange);
				activity.SetTag(MessagingTags.RoutingKey, string.IsNullOrEmpty(delivery.RoutingKey) ? _binding.RoutingKey : delivery.RoutingKey);
				activity.SetTag(MessagingTags.ConsumerGroup, _binding.QueueName);
				activity.SetTag(MessagingTags.Handler, _invoker.HandlerName);
				activity.SetTag(MessagingTags.MessageType, _invoker.MessageType.Name);
				activity.SetTag(MessagingTags.Attempt, attempt);
				if (identity is not null)
				{
					activity.SetTag(MessagingTags.MessageId, identity.MessageId.ToString());
					activity.SetTag(MessagingTags.ConversationId, identity.TraceId.ToString());
				}
			}

			return activity;
		}

		private static void RecordFailure(Activity? activity, Exception exception)
		{
			if (activity is null)
			{
				return;
			}

			activity.SetStatus(ActivityStatusCode.Error, exception.Message);
			activity.SetTag(MessagingTags.ErrorType, exception.GetType().FullName);
			activity.AddException(exception);
		}

		private IDisposable? BeginMessageScope(IMessage? identity)
		{
			if (identity is null)
			{
				return null;
			}

			// Scopes from one ILoggerFactory are shared, so loggers inside the handler see these too.
			return _logger.BeginScope(new Dictionary<string, object>
			{
				["MessageId"] = identity.MessageId,
				["TraceId"] = identity.TraceId,
			});
		}
	}
}
