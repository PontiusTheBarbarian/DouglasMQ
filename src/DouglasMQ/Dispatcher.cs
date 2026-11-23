using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Runs every consumer: opens the consumer connection, declares the dead-letter topology and starts
	/// one <see cref="Consumer"/> per <see cref="HandlerRegistration"/>; on stop, shuts them down and
	/// closes the connection.
	/// </summary>
	/// <remarks>
	/// <para>
	/// As an <see cref="IHostedService"/> (under Microsoft DI), <see cref="StartAsync"/> returns at once and
	/// set-up runs in the background, retrying with a growing delay while the broker is unreachable, so the
	/// host starts and can report the outage instead of crashing. <see cref="DouglasMQBuilder.StartConsumersAsync"/>
	/// uses <see cref="StartAndWaitAsync"/> instead, which makes one attempt and throws if it fails.
	/// </para>
	/// <para>
	/// If an attempt fails part-way, whatever it opened is released before retrying or throwing.
	/// Shutdown happens once, however many <see cref="StopAsync"/> and <see cref="DisposeAsync"/> calls
	/// overlap, and every caller waits for it to finish.
	/// </para>
	/// </remarks>
	internal sealed class Dispatcher : IHostedService, IAsyncDisposable, IDouglasMQStatus
	{
		private readonly Func<CancellationToken, Task<IConnection>> _connectionFactory;
		private readonly IReadOnlyList<HandlerRegistration> _registrations;
		private readonly ISerializer _serializer;
		private readonly Configuration _configuration;
		private readonly ILoggerFactory _loggerFactory;
		private readonly ILogger<Dispatcher> _logger;

		// Cancelled when shutdown begins, ending the background start-up loop and its retry delays.
		private readonly CancellationTokenSource _lifetime = new();
		private readonly object _gate = new();
		private readonly List<Consumer> _consumers = new();
		private IConnection? _connection;
		private Task? _startupLoop;
		private Task? _shutdownTask;
		private bool _started;
		private volatile bool _running;

		/// <param name="connectionFactory">Opens the consumer connection. The dispatcher owns the result.</param>
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

		/// <summary>Delay before the first background start-up retry. Doubles after each failure.</summary>
		internal TimeSpan InitialStartupRetryDelay { get; init; } = TimeSpan.FromSeconds(2);

		/// <summary>Upper bound on the background start-up retry delay.</summary>
		internal TimeSpan MaxStartupRetryDelay { get; init; } = TimeSpan.FromMinutes(1);

		/// <inheritdoc />
		public bool ConsumersRunning => _running;

		/// <summary>
		/// Begins starting the consumers in the background and returns immediately. Set-up retries until
		/// it succeeds or the dispatcher is stopped.
		/// </summary>
		public Task StartAsync(CancellationToken cancellationToken)
		{
			MarkStarted();
			_startupLoop = Task.Run(() => RunStartupLoopAsync(_lifetime.Token), CancellationToken.None);
			return Task.CompletedTask;
		}

		/// <summary>
		/// Starts the consumers and waits until they are running. Makes a single attempt: if it fails,
		/// everything it opened is released and the exception propagates.
		/// </summary>
		public async Task StartAndWaitAsync(CancellationToken cancellationToken)
		{
			MarkStarted();
			using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
			await StartOnceAsync(linked.Token);
			LogStarted();
		}

		private void MarkStarted()
		{
			lock (_gate)
			{
				ObjectDisposedException.ThrowIf(_shutdownTask is not null, this);
				if (_started)
				{
					throw new InvalidOperationException("The dispatcher has already been started.");
				}

				_started = true;
			}
		}

		private async Task RunStartupLoopAsync(CancellationToken stoppingToken)
		{
			var delay = InitialStartupRetryDelay;
			for (var attempt = 1; ; attempt++)
			{
				try
				{
					await StartOnceAsync(stoppingToken);
					LogStarted();
					return;
				}
				catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
				{
					return;
				}
				catch (Exception ex)
				{
					_logger.LogWarning(ex,
						"DouglasMQ could not start its consumers (attempt {Attempt}). Retrying in {RetryDelay}.",
						attempt, delay);
				}

				try
				{
					await Task.Delay(delay, stoppingToken);
				}
				catch (OperationCanceledException)
				{
					return;
				}

				delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxStartupRetryDelay.Ticks));
			}
		}

		private void LogStarted()
			=> _logger.LogInformation("DouglasMQ consumers started: {ConsumerCount} consumer(s) running.", _consumers.Count);

		/// <summary>
		/// One start-up attempt: open the connection, declare the dead-letter topology, start every consumer.
		/// On failure, releases what it opened and rethrows.
		/// </summary>
		private async Task StartOnceAsync(CancellationToken cancellationToken)
		{
			try
			{
				_connection = await _connectionFactory(cancellationToken);
				await DeclareDeadLetterAsync(_connection, cancellationToken);

				var consumerLogger = _loggerFactory.CreateLogger<Consumer>();
				foreach (var registration in _registrations)
				{
					var consumer = Consumer.Create(_connection, registration, _serializer, _configuration, consumerLogger);
					_consumers.Add(consumer);
					await consumer.StartAsync(cancellationToken);
				}

				_running = true;
			}
			catch
			{
				await ReleaseAsync(Consumer.DefaultDrainTimeout);
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
		/// Stops every consumer gracefully (letting in-flight handlers finish, bounded by
		/// <paramref name="cancellationToken"/>) and closes the consumer connection.
		/// </summary>
		public Task StopAsync(CancellationToken cancellationToken) => ShutdownAsync(cancellationToken);

		/// <summary>
		/// Stops every consumer and closes the consumer connection, if not already done or under way.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			using var timeout = new CancellationTokenSource(Consumer.DefaultDrainTimeout);
			await ShutdownAsync(timeout.Token);
		}

		private Task ShutdownAsync(CancellationToken cancellationToken)
		{
			lock (_gate)
			{
				// Every caller, overlapping or later, awaits the first caller's shutdown.
				_shutdownTask ??= Task.Run(() => ShutdownCoreAsync(cancellationToken), CancellationToken.None);
				return _shutdownTask;
			}
		}

		private async Task ShutdownCoreAsync(CancellationToken cancellationToken)
		{
			_logger.LogInformation("DouglasMQ dispatcher stopping.");
			await _lifetime.CancelAsync();

			if (_startupLoop is { } startupLoop)
			{
				// The loop exits promptly once cancelled (and has released a half-finished attempt).
				await startupLoop;
			}

			await ReleaseAsync(cancellationToken);
		}

		/// <summary>
		/// Stops the consumers (concurrently, so they share the drain timeout) and closes the connection.
		/// </summary>
		private async Task ReleaseAsync(CancellationToken drainToken)
		{
			_running = false;
			try
			{
				await Task.WhenAll(_consumers.Select(consumer => consumer.StopAsync(drainToken)));
			}
			finally
			{
				_consumers.Clear();
				var connection = _connection;
				_connection = null;
				await AmqpResources.CloseAndDisposeAsync(connection);
			}
		}

		private async Task ReleaseAsync(TimeSpan drainTimeout)
		{
			using var timeout = new CancellationTokenSource(drainTimeout);
			await ReleaseAsync(timeout.Token);
		}
	}
}
