using System;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;

namespace DouglasMQ
{
	/// <summary>
	/// Turns a message body into a <see cref="DeliveryOutcome"/>: deserializes it, runs the handler, and
	/// retries with backoff on failure. Knows nothing about channels or acknowledgements, so the
	/// processing rules can be tested without RabbitMQ.
	/// </summary>
	internal sealed class DeliveryProcessor
	{
		private readonly ISerializer _serializer;
		private readonly HandlerInvoker _invoker;
		private readonly RetryPolicy _retryPolicy;
		private readonly ILogger _logger;
		private readonly string _consumerTag;

		/// <param name="serializer">Deserializes message bodies.</param>
		/// <param name="invoker">Calls the handler.</param>
		/// <param name="retryPolicy">How often and how long apart to retry a failing handler.</param>
		/// <param name="logger">Logger for processing failures.</param>
		/// <param name="consumerTag">Identifies the consumer in log messages.</param>
		public DeliveryProcessor(ISerializer serializer, HandlerInvoker invoker, RetryPolicy retryPolicy, ILogger logger, string consumerTag)
		{
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			_invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
			_retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_consumerTag = consumerTag;
		}

		/// <summary>
		/// Processes one message body.
		/// </summary>
		/// <param name="body">The raw message body.</param>
		/// <param name="stoppingToken">
		/// Signals consumer shutdown. Cancelling it during a retry delay yields <see cref="DeliveryOutcome.Abandon"/>.
		/// A handler's own <see cref="OperationCanceledException"/> (e.g. an HTTP timeout) while this token
		/// is not cancelled counts as an ordinary failure.
		/// </param>
		public async Task<DeliveryOutcome> ProcessAsync(ReadOnlyMemory<byte> body, CancellationToken stoppingToken)
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
					_invoker.MessageType.Name, _consumerTag);
				return DeliveryOutcome.DeadLetter;
			}

			if (message is null)
			{
				_logger.LogWarning("Received null message for {MessageType} on {ConsumerTag}. Dead-lettering.",
					_invoker.MessageType.Name, _consumerTag);
				return DeliveryOutcome.DeadLetter;
			}

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
			_logger.LogInformation("Consumer {ConsumerTag} stopping; delivery will be redelivered.", _consumerTag);
			return DeliveryOutcome.Abandon;
		}
	}
}
