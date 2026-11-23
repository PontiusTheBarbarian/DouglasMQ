using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Runs every consumer: on start it opens the consumer connection, declares the dead-letter
	/// topology and starts one <see cref="Consumer"/> per <see cref="HandlerRegistration"/>; on stop it
	/// shuts them down and closes the connection. Used as an <see cref="IHostedService"/> under
	/// Microsoft DI and returned directly by <see cref="DouglasMQBuilder.StartConsumersAsync"/>.
	/// </summary>
	/// <remarks>
	/// If startup fails part-way, whatever was already opened is released before the exception
	/// propagates, so callers never have to clean up after a failed start.
	/// </remarks>
	internal sealed class Dispatcher : IHostedService, IAsyncDisposable
	{
		private readonly Func<CancellationToken, Task<IConnection>> _connectionFactory;
		private readonly IReadOnlyList<HandlerRegistration> _registrations;
		private readonly ISerializer _serializer;
		private readonly Configuration _configuration;
		private readonly ILoggerFactory _loggerFactory;
		private readonly ILogger<Dispatcher> _logger;
		private readonly List<Consumer> _consumers = new();
		private IConnection? _connection;
		private bool _disposed;

		/// <param name="connectionFactory">Opens the consumer connection. Called once, from <see cref="StartAsync"/>; the dispatcher owns the result.</param>
		/// <param name="registrations">The consumers to start.</param>
		/// <param name="serializer">Deserializes message bodies.</param>
		/// <param name="configuration">Supplies QoS and retry settings.</param>
		/// <param name="loggerFactory">Creates loggers for the dispatcher and its consumers.</param>
		public Dispatcher(
			Func<CancellationToken, Task<IConnection>> connectionFactory,
			IReadOnlyList<HandlerRegistration> registrations,
			ISerializer serializer,
			Configuration configuration,
			ILoggerFactory loggerFactory)
		{
			_connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
			_registrations = registrations ?? throw new ArgumentNullException(nameof(registrations));
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			_configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
			_loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
			_logger = loggerFactory.CreateLogger<Dispatcher>();
		}

		/// <summary>
		/// Creates a dispatcher whose consumer connection is opened from <paramref name="configuration"/>.
		/// </summary>
		public static Dispatcher Create(IReadOnlyList<HandlerRegistration> registrations, ISerializer serializer, Configuration configuration, ILoggerFactory loggerFactory)
			=> new(
				cancellationToken => ConnectionFactoryBuilder.CreateConnectionAsync(configuration, ConnectionFactoryBuilder.ConsumerSuffix, cancellationToken),
				registrations,
				serializer,
				configuration,
				loggerFactory);

		/// <summary>
		/// Opens the consumer connection, declares the dead-letter topology and starts every consumer.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			ObjectDisposedException.ThrowIf(_disposed, this);
			if (_connection is not null)
			{
				throw new InvalidOperationException("The dispatcher has already been started.");
			}

			try
			{
				_logger.LogInformation("DouglasMQ dispatcher starting. Declaring DLX topology.");
				_connection = await _connectionFactory(cancellationToken);
				await DeclareDeadLetterAsync(_connection, cancellationToken);

				_logger.LogInformation("Starting {Count} consumer(s).", _registrations.Count);
				var consumerLogger = _loggerFactory.CreateLogger<Consumer>();
				foreach (var registration in _registrations)
				{
					var consumer = Consumer.Create(_connection, registration, _serializer, _configuration, consumerLogger);
					_consumers.Add(consumer);
					await consumer.StartAsync(cancellationToken);
				}

				_logger.LogInformation("DouglasMQ dispatcher started.");
			}
			catch
			{
				// Startup failed part-way: release consumer channels and the connection rather than
				// leaking them to a caller who never received a running dispatcher.
				await ShutdownAsync();
				throw;
			}
		}

		private async Task DeclareDeadLetterAsync(IConnection connection, CancellationToken cancellationToken)
		{
			var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
			try
			{
				await Topology.DeclareDeadLetterAsync(channel, cancellationToken);
				_logger.LogDebug("DLX topology declared successfully.");
			}
			finally
			{
				await AmqpResources.CloseAndDisposeAsync(channel);
			}
		}

		/// <summary>
		/// Stops every consumer and closes the consumer connection.
		/// </summary>
		public async Task StopAsync(CancellationToken cancellationToken)
		{
			_logger.LogInformation("DouglasMQ dispatcher stopping.");
			await ShutdownAsync();
		}

		/// <summary>
		/// Stops every consumer and closes the consumer connection, if not already done.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			_disposed = true;
			await ShutdownAsync();
		}

		private async Task ShutdownAsync()
		{
			// Disposing a consumer cancels its in-flight processing token and closes its channel,
			// so unfinished deliveries are returned to the broker rather than left dangling.
			// Consumer disposal is idempotent, and the connection is detached before closing, so
			// StopAsync followed by DisposeAsync (as the host does) shuts down once.
			try
			{
				foreach (var consumer in _consumers)
				{
					await consumer.DisposeAsync();
				}
			}
			finally
			{
				_consumers.Clear();

				var connection = _connection;
				_connection = null;
				await AmqpResources.CloseAndDisposeAsync(connection);
			}
		}
	}
}
