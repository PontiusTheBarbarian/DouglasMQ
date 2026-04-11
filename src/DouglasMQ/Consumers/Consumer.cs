using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
	internal sealed class Consumer : IAsyncDisposable
	{
		private readonly ISerializer _serializer;
		private readonly IConnection _connection;
		private readonly ILogger _logger;
		private readonly int _retryCount;
		private readonly int _retryDelayMs;
		private readonly ushort _prefetchCount;
		private IChannel? _channel;
		private bool _disposed;

		private string _exchangeType { get; } = null!;
		private string _exchangeName { get; } = null!;
		private string _queueName { get; } = null!;
		private string _routingKey { get; } = null!;
		private string _consumerTag { get; } = null!;
		private bool _autoAck { get; } = false;
		private bool _noLocal { get; } = false;
		private bool _exclusive { get; } = false;
		private IDictionary<string, object?>? _arguments { get; }
		private object _concreteTypeDefinition { get; } = null!;

		private Consumer(ISerializer serializer,
			IConnection connection,
			ILogger logger,
			Configuration configuration,
			string exchangeType,
			string exchangeName,
			string queueName,
			string routingKey,
			string consumerTag,
			object concreteTypeDefinition)
		{
			_exchangeType = exchangeType;
			_exchangeName = exchangeName;
			_queueName = queueName;
			_routingKey = routingKey;
			_consumerTag = consumerTag;
			_serializer = serializer;
			_connection = connection;
			_logger = logger;
			_retryCount = configuration.RetryCount;
			_retryDelayMs = configuration.RetryDelayMs;
			_prefetchCount = configuration.PrefetchCount;
			_concreteTypeDefinition = concreteTypeDefinition;
		}

		/// <summary>
		/// Creates a new <see cref="Consumer"/> from a <see cref="HandlerAttribute"/> and its handler instance.
		/// </summary>
		public static Consumer Create(IConnection connection,
			ISerializer serializer,
			ILogger logger,
			Configuration configuration,
			HandlerAttribute attribute,
			object concreteTypeDefinition)
			=> new Consumer(serializer,
				connection,
				logger,
				configuration,
				attribute.ExchangeType,
				attribute.ExchangeName,
				attribute.QueueName,
				attribute.RoutingKey,
				attribute.ConsumerTag,
				concreteTypeDefinition);

		/// <summary>
		/// Declares topology, sets QoS, and begins consuming messages.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			_channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
			await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetchCount, global: false, cancellationToken: cancellationToken);
			await DeclareRemoteAsync(_channel, cancellationToken);
			await StartConsumingAsync(_channel, cancellationToken);
		}

		private async Task DeclareRemoteAsync(IChannel channel, CancellationToken cancellationToken)
		{
			await channel.ExchangeDeclareAsync(_exchangeName, _exchangeType, durable: true, autoDelete: false, cancellationToken: cancellationToken);
			await channel.QueueDeclareAsync(_queueName, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>()
			{
				{ "x-dead-letter-exchange", "dlx.exchange" },
				{ "x-dead-letter-routing-key", "dlx_routing_key" }
			}, cancellationToken: cancellationToken);
			await channel.QueueBindAsync(_queueName, _exchangeName, _routingKey, cancellationToken: cancellationToken);
		}

		private async Task StartConsumingAsync(IChannel channel, CancellationToken cancellationToken)
		{
			var consumer = new AsyncEventingBasicConsumer(channel);
			consumer.ReceivedAsync += BasicConsume;

			await channel.BasicConsumeAsync(
				queue: _queueName,
				autoAck: _autoAck,
				consumer: consumer,
				consumerTag: _consumerTag,
				noLocal: _noLocal,
				exclusive: _exclusive,
				arguments: _arguments,
				cancellationToken: cancellationToken);

			async Task BasicConsume(object sender, BasicDeliverEventArgs eventArgs)
			{
				const string HandleAsync = nameof(HandleAsync);

				var handleMethod = _concreteTypeDefinition.GetType().GetMethod(HandleAsync);
				if (handleMethod is null)
				{
					throw new InvalidOperationException($"Method '{HandleAsync}' not found on {_concreteTypeDefinition.GetType()}.");
				}

				var parameter = handleMethod.GetParameters().Single(ParameterContainsMessageAttribute);
				var json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
				var message = _serializer.Deserialize(json!, parameter.ParameterType);
				if (message is null)
				{
					_logger.LogWarning("Received null or undeserializable message for {ParameterType}. Dead-lettering.", parameter.ParameterType);
					await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: cancellationToken);
					return;
				}

				var lastException = default(Exception);
				for (var attempt = 0; attempt <= _retryCount; attempt++)
				{
					try
					{
						if (attempt > 0)
						{
							var delayMs = _retryDelayMs * (int)Math.Pow(2, attempt - 1);
							_logger.LogWarning("Retrying handler {Handler} for message {MessageType}, attempt {Attempt}/{RetryCount} after {DelayMs}ms.",
								_concreteTypeDefinition.GetType().Name, parameter.ParameterType.Name, attempt, _retryCount, delayMs);
							await Task.Delay(delayMs, cancellationToken);
						}

						await (Task)handleMethod.Invoke(_concreteTypeDefinition, new object[] { message })!;
						await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
						return;
					}
					catch (OperationCanceledException)
					{
						throw;
					}
					catch (Exception ex)
					{
						lastException = ex;
					}
				}

				_logger.LogError(lastException,
					"Handler {Handler} failed for message {MessageType} after {Attempts} attempt(s). Dead-lettering.",
					_concreteTypeDefinition.GetType().Name, parameter.ParameterType.Name, _retryCount + 1);

				await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken: cancellationToken);
			}
		}

		/// <summary>
		/// Asynchronously disposes of the resources used by the <see cref="Consumer"/>.
		/// </summary>
		public async ValueTask DisposeAsync(bool disposing)
		{
			if (!disposing || _disposed || _channel is null)
			{
				return;
			}

			try
			{
				await _channel.CloseAsync();
			}
			finally
			{
				await _channel.DisposeAsync();
				_disposed = true;
			}
		}

		/// <summary>
		/// Disposes of the <see cref="Consumer"/> and suppresses finalization.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			await DisposeAsync(true);
			GC.SuppressFinalize(this);
		}

		private readonly Func<ParameterInfo, bool> ParameterContainsMessageAttribute = p =>
			p.ParameterType.GetCustomAttributes(typeof(MessageAttribute), true).Any();
	}
}
