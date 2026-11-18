using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Manages the lifecycle of message consumers in a message broker system.
	/// The <see cref="Dispatcher"/> handles starting and stopping consumers, as well as managing
	/// the connection lifecycle. Implements <see cref="IHostedService"/> to integrate with the
	/// application's hosting infrastructure and <see cref="IAsyncDisposable"/> for proper resource cleanup.
	/// </summary>
	internal sealed class Dispatcher : IHostedService, IAsyncDisposable
	{
		private bool _disposed;
		private readonly IConnection _connection;
		private readonly List<Consumer> _consumers;
		private readonly ILogger<Dispatcher> _logger;

		/// <summary>
		/// Initializes a new instance of the <see cref="Dispatcher"/> class.
		/// </summary>
		/// <param name="connection">The RabbitMQ connection to use for creating channels.</param>
		/// <param name="consumers">A list of consumers to manage.</param>
		/// <param name="logger">Logger for operational events.</param>
		public Dispatcher(IConnection connection, List<Consumer> consumers, ILogger<Dispatcher> logger)
		{
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
			_consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		}

		/// <summary>
		/// Starts the <see cref="Dispatcher"/> and initializes all registered consumers.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			_logger.LogInformation("DouglasMQ dispatcher starting. Declaring DLX topology.");
			await DeclareDLXAsync(cancellationToken);

			_logger.LogInformation("Starting {Count} consumer(s).", _consumers.Count);
			foreach (var consumer in _consumers)
			{
				await consumer.StartAsync(cancellationToken);
			}

			_logger.LogInformation("DouglasMQ dispatcher started.");
		}

		private async Task DeclareDLXAsync(CancellationToken cancellationToken)
		{
			var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
			try
			{
				await channel.ExchangeDeclareAsync("dlx.exchange", ExchangeType.direct.ToString(), durable: true, autoDelete: false, cancellationToken: cancellationToken);
				await channel.QueueDeclareAsync("dlx.queue", durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
				await channel.QueueBindAsync("dlx.queue", "dlx.exchange", "dlx_routing_key", cancellationToken: cancellationToken);
				_logger.LogDebug("DLX topology declared successfully.");
			}
			finally
			{
				await channel.CloseAsync(cancellationToken);
				await channel.DisposeAsync();
			}
		}

		/// <summary>
		/// Stops the <see cref="Dispatcher"/> and closes the connection.
		/// </summary>
		public async Task StopAsync(CancellationToken cancellationToken)
		{
			_logger.LogInformation("DouglasMQ dispatcher stopping.");
			await StopConsumersAsync();

			if (_connection.IsOpen)
			{
				await _connection.CloseAsync(cancellationToken);
			}
		}

		private async Task StopConsumersAsync()
		{
			// Disposing a consumer cancels its in-flight processing token and closes its channel,
			// so unfinished deliveries are returned to the broker rather than left dangling.
			foreach (var consumer in _consumers)
			{
				await consumer.DisposeAsync();
			}
		}

		/// <summary>
		/// Asynchronously disposes of the resources used by the <see cref="Dispatcher"/>.
		/// </summary>
		public async ValueTask DisposeAsync(bool disposing)
		{
			if (!disposing || _disposed)
			{
				return;
			}

			try
			{
				await StopConsumersAsync();

				if (_connection.IsOpen)
				{
					await _connection.CloseAsync();
				}
			}
			finally
			{
				await _connection.DisposeAsync();
				_disposed = true;
			}
		}

		/// <summary>
		/// Disposes of the <see cref="Dispatcher"/> and suppresses finalization.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			await DisposeAsync(true);
			GC.SuppressFinalize(this);
		}
	}
}
