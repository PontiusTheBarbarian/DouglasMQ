using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Provides functionality to publish events and send commands to a message broker.
	/// Reuses a single long-lived channel and optionally enables broker publisher confirms.
	/// </summary>
	public sealed class Publisher : IPublisher, IAsyncDisposable
	{
		private readonly IConnection _connection;
		private readonly ISerializer _serializer;
		private readonly ILogger<Publisher> _logger;
		private readonly bool _publisherConfirms;
		private readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);
		private volatile IChannel? _channel;
		private bool _disposed;

		/// <summary>
		/// Initializes a new instance of the <see cref="Publisher"/> class.
		/// </summary>
		public Publisher(IConnection connection, ISerializer serializer, ILogger<Publisher> logger, Configuration configuration)
		{
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_publisherConfirms = configuration?.PublisherConfirms ?? false;
		}

		/// <summary>
		/// Publishes an event to the message broker.
		/// </summary>
		/// <typeparam name="TEvent">The type of the event to be published. Must implement <see cref="IEvent"/>.</typeparam>
		/// <param name="event">The event to be published.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="event"/> is <c>null</c>.</exception>
		/// <exception cref="InvalidOperationException">Thrown when the message type does not have a <see cref="MessageAttribute"/>.</exception>
		public async ValueTask PublishAsync<TEvent>(TEvent @event)
			where TEvent : class, IEvent
		{
			if (@event is null)
			{
				throw new ArgumentNullException(nameof(@event));
			}

			var routingInformation = GetMessageAttribute<TEvent>();
			await PublishCoreAsync<TEvent>(@event, routingInformation.ExchangeName, routingInformation.RoutingKey);
		}

		/// <summary>
		/// Sends a command to the message broker.
		/// </summary>
		/// <typeparam name="TCommand">The type of the command to be sent. Must implement <see cref="ICommand"/>.</typeparam>
		/// <param name="command">The command to be sent.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="command"/> is <c>null</c>.</exception>
		/// <exception cref="InvalidOperationException">Thrown when the message type does not have a <see cref="MessageAttribute"/>.</exception>
		public async ValueTask SendAsync<TCommand>(TCommand command)
			where TCommand : class, ICommand
		{
			if (command is null)
			{
				throw new ArgumentNullException(nameof(command));
			}

			var routingInformation = GetMessageAttribute<TCommand>();
			await PublishCoreAsync<TCommand>(command, routingInformation.ExchangeName, routingInformation.RoutingKey);
		}

		private static MessageAttribute GetMessageAttribute<TMessage>()
		{
			var attr = Attribute.GetCustomAttributes(typeof(TMessage))
				.OfType<MessageAttribute>()
				.SingleOrDefault();

			if (attr is null)
			{
				throw new InvalidOperationException(
					$"Message type '{typeof(TMessage).Name}' is missing the [Message] attribute.");
			}

			return attr;
		}

		private async ValueTask PublishCoreAsync<TMessage>(TMessage message, string exchangeName, string routingKey)
			where TMessage : class, IMessage
		{
			ObjectDisposedException.ThrowIf(_disposed, this);

			var channel = await GetOrCreateChannelAsync();
			var body = Encoding.UTF8.GetBytes(_serializer.Serialize<TMessage>(message));

			// Persistent (delivery mode 2) so messages survive a broker restart when routed to
			// durable queues. Identity/correlation are surfaced as standard AMQP properties so
			// they are visible to tooling and non-DouglasMQ consumers without deserializing the body.
			var properties = new BasicProperties
			{
				Persistent = true,
				MessageId = message.MessageId.ToString(),
				CorrelationId = message.TraceId.ToString(),
				Type = typeof(TMessage).FullName,
				Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
			};

			_logger.LogDebug("Publishing {MessageType} {MessageId} to exchange '{Exchange}' with routing key '{RoutingKey}'.",
				typeof(TMessage).Name, message.MessageId, exchangeName, routingKey);

			await channel.BasicPublishAsync(
				exchange: exchangeName,
				routingKey: routingKey,
				mandatory: false,
				basicProperties: properties,
				body: body);
		}

		private async ValueTask<IChannel> GetOrCreateChannelAsync()
		{
			var channel = _channel;
			if (channel is { IsOpen: true })
			{
				return channel;
			}

			await _channelLock.WaitAsync();
			try
			{
				if (_channel is { IsOpen: true })
				{
					return _channel;
				}

				if (_channel is not null)
				{
					// A channel-level error (e.g. publishing to a missing exchange) makes the broker
					// close the channel, and automatic recovery does not reopen it. Replace it so one
					// bad publish does not poison every publish that follows.
					_logger.LogWarning("Publisher channel was closed ({CloseReason}). Creating a new channel.", _channel.CloseReason);
					var stale = _channel;
					_channel = null;
					await stale.DisposeAsync();
				}

				_logger.LogDebug("Creating publisher channel (publisherConfirms={PublisherConfirms}).", _publisherConfirms);

				// In RabbitMQ.Client 7.x publisher confirms are enabled via CreateChannelOptions,
				// not via ConfirmSelectAsync. When enabled the broker will ACK each BasicPublishAsync.
				var options = new CreateChannelOptions(
					publisherConfirmationsEnabled: _publisherConfirms,
					publisherConfirmationTrackingEnabled: _publisherConfirms);

				_channel = await _connection.CreateChannelAsync(options);
				return _channel;
			}
			finally
			{
				_channelLock.Release();
			}
		}

		/// <summary>
		/// Closes and disposes the publisher channel.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			if (_disposed)
			{
				return;
			}

			_disposed = true;

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

			_channelLock.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
