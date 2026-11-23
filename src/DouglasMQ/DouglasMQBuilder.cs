using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Fluent builder for creating a DouglasMQ publisher and/or starting consumers without
	/// requiring a dependency injection container.
	/// </summary>
	/// <example>
	/// <code>
	/// var builder = new DouglasMQBuilder()
	///     .Configure(c => c.HostName = "localhost")
	///     .WithSerializer(new SystemTextJsonSerializer()) // optional: this is the default
	///     .AddHandler(new OrderPlacedHandler());
	///
	/// await using var publisher = await builder.BuildPublisherAsync();
	/// await publisher.PublishAsync(new MyEvent());
	/// </code>
	/// </example>
	public sealed class DouglasMQBuilder
	{
		private Configuration _configuration = new Configuration();
		private ISerializer _serializer = new SystemTextJsonSerializer();
		private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
		private readonly List<HandlerRegistration> _registrations = new();

		/// <summary>
		/// Applies settings to the <see cref="Configuration"/> using a configuration delegate.
		/// </summary>
		public DouglasMQBuilder Configure(Action<Configuration> configure)
		{
			configure(_configuration);
			return this;
		}

		/// <summary>The serializer that will be used for message bodies.</summary>
		internal ISerializer Serializer => _serializer;

		/// <summary>
		/// Replaces the serializer used to encode and decode message bodies. Defaults to
		/// <see cref="SystemTextJsonSerializer"/>.
		/// </summary>
		public DouglasMQBuilder WithSerializer(ISerializer serializer)
		{
			_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
			return this;
		}

		/// <summary>
		/// Provides a logger factory for internal logging.
		/// </summary>
		public DouglasMQBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
		{
			_loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
			return this;
		}

		/// <summary>
		/// Registers a handler instance directly. The handler class must be decorated with
		/// <see cref="HandlerAttribute"/> and implement exactly one <see cref="IHandler{TMessage}"/>.
		/// </summary>
		/// <exception cref="InvalidOperationException">
		/// The handler is not decorated with <see cref="HandlerAttribute"/>, or does not implement exactly
		/// one <see cref="IHandler{TMessage}"/>.
		/// </exception>
		public DouglasMQBuilder AddHandler(object handler)
		{
			ArgumentNullException.ThrowIfNull(handler);

			_registrations.Add(HandlerRegistration.For(HandlerInvoker.Create(handler)));
			return this;
		}

		/// <summary>
		/// Scans the assembly containing <typeparamref name="TMarker"/> for all types that
		/// implement <see cref="IHandler{TMessage}"/> and are decorated with <see cref="HandlerAttribute"/>,
		/// and registers them using the provided factory to resolve each instance.
		/// </summary>
		public DouglasMQBuilder AddHandlersFromAssembly<TMarker>(Func<Type, object> handlerFactory)
		{
			ArgumentNullException.ThrowIfNull(handlerFactory);

			foreach (var handlerType in HandlerDiscovery.FindHandlerTypes(typeof(TMarker).Assembly))
			{
				AddHandler(handlerFactory(handlerType));
			}

			return this;
		}

		/// <summary>
		/// Builds a <see cref="IPublisher"/> with its own connection to the configured broker.
		/// The publisher owns that connection: disposing the publisher closes it.
		/// </summary>
		public async Task<IPublisher> BuildPublisherAsync(CancellationToken cancellationToken = default)
		{
			var publisher = Publisher.Create(_configuration, _serializer, _loggerFactory.CreateLogger<Publisher>());
			try
			{
				// Connect now so an unreachable broker fails the build, not the first publish.
				await publisher.ConnectAsync(cancellationToken);
				return publisher;
			}
			catch
			{
				await publisher.DisposeAsync();
				throw;
			}
		}

		/// <summary>
		/// Opens a dedicated consumer connection, declares DLX topology, starts all registered
		/// consumers and returns an <see cref="IAsyncDisposable"/> that stops them and closes the
		/// connection on disposal. Unlike <c>AddDouglasMQ</c>, this waits until the consumers are running
		/// and throws if the broker is unreachable; everything opened so far is then released.
		/// </summary>
		/// <remarks>
		/// Disposing the result stops consumers gracefully: in-flight handlers get up to 30 seconds to
		/// finish and settle. The result also implements <see cref="IDouglasMQStatus"/>.
		/// </remarks>
		public async Task<IAsyncDisposable> StartConsumersAsync(CancellationToken cancellationToken = default)
		{
			var dispatcher = Dispatcher.Create(_registrations.ToList(), _serializer, _configuration, _loggerFactory);
			await dispatcher.StartAndWaitAsync(cancellationToken);
			return dispatcher;
		}

		/// <summary>
		/// Builds a publisher and starts all consumers. Each uses its own connection (as RabbitMQ
		/// recommends, so publisher flow control cannot stall consumers), and each is disposed
		/// independently: stopping the consumers does not affect the publisher.
		/// </summary>
		public async Task<(IPublisher Publisher, IAsyncDisposable Consumers)> BuildAsync(CancellationToken cancellationToken = default)
		{
			var publisher = await BuildPublisherAsync(cancellationToken);
			try
			{
				var consumers = await StartConsumersAsync(cancellationToken);
				return (publisher, consumers);
			}
			catch
			{
				await ((IAsyncDisposable)publisher).DisposeAsync();
				throw;
			}
		}

		/// <summary>
		/// Creates and returns a raw RabbitMQ connection using the current configuration.
		/// The caller is responsible for closing and disposing the connection.
		/// </summary>
		public Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
			=> ConnectionFactoryBuilder.CreateConnectionAsync(_configuration, purpose: null, cancellationToken);
	}
}
