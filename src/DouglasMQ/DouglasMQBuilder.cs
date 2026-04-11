using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
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
	///     .WithSerializer(new MySerializer())
	///     .AddHandler(new ManHandler());
	///
	/// await using var publisher = await builder.BuildPublisherAsync();
	/// await publisher.PublishAsync(new MyEvent());
	/// </code>
	/// </example>
	public sealed class DouglasMQBuilder
	{
		private Configuration _configuration = new Configuration();
		private ISerializer? _serializer;
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

		/// <summary>
		/// Provides the serializer used to encode and decode message bodies.
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
			var handlers = typeof(TMarker).Assembly
				.GetTypes()
				.Where(t =>
					t.GetCustomAttribute<HandlerAttribute>() is not null &&
					t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandler<>)));

			foreach (var handlerType in handlers)
			{
				var attribute = handlerType.GetCustomAttribute<HandlerAttribute>()!;
				var instance = handlerFactory(handlerType);
				_handlers.Add((attribute, instance));
			}

			return this;
		}

		/// <summary>
		/// Builds a <see cref="IPublisher"/> connected to the configured broker.
		/// The caller is responsible for disposing the returned publisher.
		/// </summary>
		public async Task<IPublisher> BuildPublisherAsync(CancellationToken cancellationToken = default)
		{
			EnsureSerializer();
			var connection = await CreateConnectionAsync(cancellationToken);
			return new Publisher(connection, _serializer!, _loggerFactory.CreateLogger<Publisher>(), _configuration);
		}

		/// <summary>
		/// Creates the RabbitMQ connection, declares DLX topology, starts all registered consumers
		/// and returns an <see cref="IAsyncDisposable"/> that stops them on disposal.
		/// </summary>
		public async Task<IAsyncDisposable> StartConsumersAsync(CancellationToken cancellationToken = default)
		{
			EnsureSerializer();
			var connection = await CreateConnectionAsync(cancellationToken);
			var dispatcher = new Dispatcher(
				connection,
				BuildConsumers(connection),
				_loggerFactory.CreateLogger<Dispatcher>());

			await dispatcher.StartAsync(cancellationToken);
			return dispatcher;
		}

		/// <summary>
		/// Builds a connected <see cref="IPublisher"/> and starts all consumers simultaneously,
		/// returning both so the caller can publish and cleanly shut down consumers.
		/// </summary>
		public async Task<(IPublisher Publisher, IAsyncDisposable Consumers)> BuildAsync(CancellationToken cancellationToken = default)
		{
			EnsureSerializer();
			var connection = await CreateConnectionAsync(cancellationToken);

			var publisher = new Publisher(connection, _serializer!, _loggerFactory.CreateLogger<Publisher>(), _configuration);
			var dispatcher = new Dispatcher(
				connection,
				BuildConsumers(connection),
				_loggerFactory.CreateLogger<Dispatcher>());

			await dispatcher.StartAsync(cancellationToken);
			return (publisher, dispatcher);
		}

		/// <summary>
		/// Creates and returns a raw RabbitMQ connection using the current configuration.
		/// The caller is responsible for closing and disposing the connection.
		/// </summary>
		public async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
		{
			var factory = new ConnectionFactory
			{
				UserName = _configuration.Username,
				Password = _configuration.Password,
				VirtualHost = _configuration.VirtualHost,
				ClientProvidedName = _configuration.ClientProvidedName,
				HostName = _configuration.HostName,
				AutomaticRecoveryEnabled = true,
				TopologyRecoveryEnabled = true,
				RequestedHeartbeat = TimeSpan.FromSeconds(60),
			};

			if (_configuration.UseSsl && _configuration.SslCertificateThumbprint is not null)
			{
				var certBytes = Convert.FromBase64String(_configuration.SslCertificateThumbprint);
				factory.Ssl = new SslOption
				{
					Enabled = true,
					Certs = new X509CertificateCollection
					{
						X509CertificateLoader.LoadCertificate(certBytes)
					},
					AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors,
				};
			}

			return await factory.CreateConnectionAsync(cancellationToken);
		}

		private List<Consumer> BuildConsumers(IConnection connection)
		{
			EnsureSerializer();
			return _handlers
				.Select(h => Consumer.Create(
					connection,
					_serializer!,
					_loggerFactory.CreateLogger<Consumer>(),
					_configuration,
					h.Attribute,
					h.Handler))
				.ToList();
		}

		private void EnsureSerializer()
		{
			if (_serializer is null)
			{
				throw new InvalidOperationException(
					"A serializer must be provided via WithSerializer() before building.");
			}
		}
	}
}
