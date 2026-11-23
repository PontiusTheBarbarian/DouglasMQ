using System;
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
		private readonly Func<CancellationToken, Task<IConnection>> _connectionFactory;
		private readonly bool _ownsConnection;
		private readonly ISerializer _serializer;
		private readonly ILogger<Publisher> _logger;
		private readonly bool _publisherConfirms;
		private readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);
		private IConnection? _connection;
		private volatile IChannel? _channel;
		private bool _disposed;

		/// <summary>
		/// Initializes a new instance of the <see cref="Publisher"/> class over a connection owned by
		/// the caller. Disposing the publisher closes its channel but leaves the connection open.
		/// </summary>
		public Publisher(IConnection connection, ISerializer serializer, ILogger<Publisher> logger, Configuration configuration)
			: this(ExistingConnection(connection ?? throw new ArgumentNullException(nameof(connection))), ownsConnection: false, serializer, logger, configuration)
		{
		}

		/// <summary>
		/// Creates a publisher that obtains its connection from <paramref name="connectionFactory"/> on
		/// first use (retrying on the next publish if that fails).
		/// </summary>
		/// <param name="connectionFactory">Supplies the connection.</param>
		/// <param name="ownsConnection">Whether disposing the publisher also closes the connection.</param>
		/// <param name="serializer">Serializes message bodies.</param>
		/// <param name="logger">Logger for operational events.</param>
		/// <param name="configuration">Supplies <see cref="Configuration.PublisherConfirms"/>.</param>
		internal Publisher(Func<CancellationToken, Task<IConnection>> connectionFactory, bool ownsConnection, ISerializer serializer, ILogger<Publisher> logger, Configuration? configuration)
		{
			_connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
			_ownsConnection = ownsConnection;
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_publisherConfirms = configuration?.PublisherConfirms ?? false;
		}

		/// <summary>
		/// Creates a publisher that opens (and owns) its own connection to the broker described by
		/// <paramref name="configuration"/>. Nothing touches the network until the first publish, or
		/// <see cref="ConnectAsync"/>.
		/// </summary>
		internal static Publisher Create(Configuration configuration, ISerializer serializer, ILogger<Publisher> logger)
			=> new(
				cancellationToken => ConnectionFactoryBuilder.CreateConnectionAsync(configuration, ConnectionFactoryBuilder.PublisherSuffix, cancellationToken),
				ownsConnection: true,
				serializer,
				logger,
				configuration);

		private static Func<CancellationToken, Task<IConnection>> ExistingConnection(IConnection connection)
			=> _ => Task.FromResult(connection);

		/// <summary>
		/// Publishes an event to the message broker.
		/// </summary>
		/// <typeparam name="TEvent">The type of the event to be published. Must implement <see cref="IEvent"/>.</typeparam>
		/// <param name="event">The event to be published.</param>
		/// <param name="cancellationToken">Cancels opening the connection or channel, and waiting for a publisher confirm.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="event"/> is <c>null</c>.</exception>
		/// <exception cref="InvalidOperationException">Thrown when the message type does not have a <see cref="MessageAttribute"/>.</exception>
		public ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
			where TEvent : class, IEvent
			=> PublishCoreAsync(@event ?? throw new ArgumentNullException(nameof(@event)), cancellationToken);

		/// <summary>
		/// Sends a command to the message broker.
		/// </summary>
		/// <typeparam name="TCommand">The type of the command to be sent. Must implement <see cref="ICommand"/>.</typeparam>
		/// <param name="command">The command to be sent.</param>
		/// <param name="cancellationToken">Cancels opening the connection or channel, and waiting for a publisher confirm.</param>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="command"/> is <c>null</c>.</exception>
		/// <exception cref="InvalidOperationException">Thrown when the message type does not have a <see cref="MessageAttribute"/>.</exception>
		public ValueTask SendAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
			where TCommand : class, ICommand
			=> PublishCoreAsync(command ?? throw new ArgumentNullException(nameof(command)), cancellationToken);

		/// <summary>
		/// Opens the connection and channel now rather than on the first publish, so an unreachable
		/// broker or bad credentials surface immediately.
		/// </summary>
		internal async Task ConnectAsync(CancellationToken cancellationToken)
		{
			ObjectDisposedException.ThrowIf(_disposed, this);
			await GetOrCreateChannelAsync(cancellationToken);
		}

		private async ValueTask PublishCoreAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
			where TMessage : class, IMessage
		{
			ObjectDisposedException.ThrowIf(_disposed, this);

			// Resolve the route before touching the network, so a missing [Message] fails fast.
			var route = MessageRoute<TMessage>.Require();
			var channel = await GetOrCreateChannelAsync(cancellationToken);
			var body = _serializer.SerializeToBytes(message);

			// Persistent (delivery mode 2) so messages survive a broker restart when routed to
			// durable queues. Identity/correlation are surfaced as standard AMQP properties so
			// they are visible to tooling and non-DouglasMQ consumers without deserializing the body.
			var properties = new BasicProperties
			{
				Persistent = true,
				MessageId = message.MessageId.ToString(),
				CorrelationId = message.TraceId.ToString(),
				Type = MessageRoute<TMessage>.TypeName,
				Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
			};

			_logger.LogDebug("Publishing {MessageType} {MessageId} to exchange '{Exchange}' with routing key '{RoutingKey}'.",
				typeof(TMessage).Name, message.MessageId, route.ExchangeName, route.RoutingKey);

			await channel.BasicPublishAsync(
				exchange: route.ExchangeName,
				routingKey: route.RoutingKey,
				mandatory: false,
				basicProperties: properties,
				body: body,
				cancellationToken: cancellationToken);
		}

		private async ValueTask<IChannel> GetOrCreateChannelAsync(CancellationToken cancellationToken)
		{
			var channel = _channel;
			if (channel is { IsOpen: true })
			{
				return channel;
			}

			await _channelLock.WaitAsync(cancellationToken);
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

				_connection ??= await _connectionFactory(cancellationToken);
				_channel = await _connection.CreateChannelAsync(options, cancellationToken);
				return _channel;
			}
			finally
			{
				_channelLock.Release();
			}
		}

		/// <summary>
		/// Closes and disposes the publisher channel, and the connection too if this publisher owns it.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			if (_disposed)
			{
				return;
			}

			_disposed = true;

			try
			{
				await AmqpResources.CloseAndDisposeAsync(_channel);
			}
			finally
			{
				if (_ownsConnection)
				{
					await AmqpResources.CloseAndDisposeAsync(_connection);
				}

				_channelLock.Dispose();
			}
		}
	}
}
