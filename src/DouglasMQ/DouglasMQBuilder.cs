using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
		private readonly List<(HandlerAttribute Attribute, object Handler)> _handlers = new();

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
		/// <see cref="HandlerAttribute"/>.
		/// </summary>
		public DouglasMQBuilder AddHandler(object handler)
		{
			if (handler is null)
			{
				throw new ArgumentNullException(nameof(handler));
			}

			var attribute = handler.GetType().GetCustomAttribute<HandlerAttribute>();
			if (attribute is null)
			{
				throw new InvalidOperationException(
					$"Handler '{handler.GetType().Name}' must be decorated with [{nameof(HandlerAttribute)}].");
			}

			_handlers.Add((attribute, handler));
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
				var attribute = handlerType.GetCustomAttribute<HandlerAttribute>()!;
				var instance = handlerFactory(handlerType);
				_handlers.Add((attribute, instance));
			}

			return this;
		}

		/// <summary>
		/// Builds a <see cref="IPublisher"/> with its own connection to the configured broker.
		/// The publisher owns that connection: disposing the publisher closes it.
		/// </summary>
		public async Task<IPublisher> BuildPublisherAsync(CancellationToken cancellationToken = default)
		{
			var connection = await ConnectionFactoryBuilder.CreateConnectionAsync(_configuration, ConnectionFactoryBuilder.PublisherSuffix, cancellationToken);
			return new Publisher(connection, ownsConnection: true, _serializer, _loggerFactory.CreateLogger<Publisher>(), _configuration);
		}

		/// <summary>
		/// Opens a dedicated consumer connection, declares DLX topology, starts all registered
		/// consumers and returns an <see cref="IAsyncDisposable"/> that stops them and closes the
		/// connection on disposal.
		/// </summary>
		public async Task<IAsyncDisposable> StartConsumersAsync(CancellationToken cancellationToken = default)
		{
			var connection = await ConnectionFactoryBuilder.CreateConnectionAsync(_configuration, ConnectionFactoryBuilder.ConsumerSuffix, cancellationToken);

			Dispatcher? dispatcher = null;
			try
			{
				dispatcher = new Dispatcher(
					connection,
					BuildConsumers(connection),
					_loggerFactory.CreateLogger<Dispatcher>());

				await dispatcher.StartAsync(cancellationToken);
				return dispatcher;
			}
			catch
			{
				// Startup failed part-way: release consumer channels and the connection rather
				// than leaking them to the caller who never received a handle.
				if (dispatcher is not null)
				{
					await dispatcher.DisposeAsync();
				}
				else
				{
					await connection.DisposeAsync();
				}

				throw;
			}
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

		private List<Consumer> BuildConsumers(IConnection connection)
		{
			return _handlers
				.Select(h => Consumer.Create(
					connection,
					_serializer,
					_loggerFactory.CreateLogger<Consumer>(),
					_configuration,
					h.Attribute,
					h.Handler))
				.ToList();
		}
	}
}
