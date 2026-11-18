using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Extension methods for registering DouglasMQ services with Microsoft.Extensions.DependencyInjection.
	/// </summary>
	public static class ServiceCollectionExtensions
	{
		private const string DouglasMQ = nameof(DouglasMQ);

		/// <summary>
		/// Registers DouglasMQ services using a configuration delegate. A serializer must be
		/// registered separately before calling this method (e.g. via <see cref="AddSerializer"/>).
		/// </summary>
		public static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			Action<Configuration, IServiceCollection> configure)
		{
			var config = new Configuration();
			configure(config, services);
			return services.AddDouglasMQ(config);
		}

		/// <summary>
		/// Registers DouglasMQ services using <see cref="IConfiguration"/> and a serializer type.
		/// </summary>
		public static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			IConfiguration configuration,
			Type serializer,
			Action<IServiceCollection>? configure = null)
		{
			var config = new Configuration();
			configuration.GetSection(DouglasMQ).Bind(config);
			configure?.Invoke(services);

			return services
				.AddSerializer(serializer)
				.AddDouglasMQ(config);
		}

		/// <summary>
		/// Registers DouglasMQ services using <see cref="IConfiguration"/>, an assembly marker type
		/// for auto-discovery of handlers, and a serializer type.
		/// </summary>
		public static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			IConfiguration configuration,
			Type mark,
			Type serializer)
		{
			var config = new Configuration();
			configuration.GetSection(DouglasMQ).Bind(config);

			return services
				.AddSerializer(serializer)
				.AutoRegisterConsumers(mark)
				.AddDouglasMQ(config);
		}

		/// <summary>
		/// Scans the assembly containing <paramref name="assemblyMark"/> for all types that
		/// implement <see cref="IHandler{TMessage}"/> and are decorated with <see cref="HandlerAttribute"/>,
		/// then registers each as a singleton.
		/// </summary>
		/// <exception cref="InvalidOperationException">
		/// A decorated handler implements more than one <see cref="IHandler{TMessage}"/>. A handler
		/// class maps to a single queue, so each message type needs its own handler class (or an
		/// explicit <see cref="AddConsumer{TMessage, TImp}"/> registration).
		/// </exception>
		public static IServiceCollection AutoRegisterConsumers(this IServiceCollection services, Type assemblyMark)
		{
			ArgumentNullException.ThrowIfNull(assemblyMark);

			foreach (var handlerType in HandlerDiscovery.FindHandlerTypes(assemblyMark.Assembly))
			{
				var messageTypes = HandlerDiscovery.GetHandledMessageTypes(handlerType);
				if (messageTypes.Count > 1)
				{
					throw new InvalidOperationException(
						$"Handler '{handlerType.Name}' implements {messageTypes.Count} IHandler<T> interfaces but has a single " +
						$"[{nameof(HandlerAttribute)}] queue, so its messages would compete on one queue. Split it into one " +
						$"handler class per message type, or register it explicitly with {nameof(AddConsumer)}<TMessage, THandler>().");
				}

				// TryAddEnumerable: re-scanning, or scanning plus AddConsumer, must not register the
				// same handler twice (which would start two consumers for it).
				services.TryAddEnumerable(ServiceDescriptor.Singleton(typeof(IHandler<>).MakeGenericType(messageTypes[0]), handlerType));
			}

			return services;
		}

		/// <summary>
		/// Manually registers a single handler.
		/// </summary>
		public static IServiceCollection AddConsumer<TMessage, TImp>(this IServiceCollection services)
			where TMessage : class, IMessage
			where TImp : class, IHandler<TMessage>
		{
			services.TryAddEnumerable(ServiceDescriptor.Singleton<IHandler<TMessage>, TImp>());
			return services;
		}

		/// <summary>
		/// Registers a serializer implementation.
		/// </summary>
		public static IServiceCollection AddSerializer(this IServiceCollection services, Type serializer)
			=> services.AddSingleton(typeof(ISerializer), serializer);

		private static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			Configuration configuration) => services
				.AddSingleton(configuration)
				// The consumer connection, owned and closed by the hosted Dispatcher. Resolved when the
				// host constructs the Dispatcher.
				.AddSingleton<IConnection>(_ =>
					ConnectionFactoryBuilder
						.CreateConnectionAsync(configuration, ConnectionFactoryBuilder.ConsumerSuffix, CancellationToken.None)
						.GetAwaiter()
						.GetResult())
				.AddSingleton<List<Consumer>>(provider => CreateConsumers(services, provider, configuration))
				// The publisher gets its own lazily-opened connection rather than sharing the consumer
				// connection, so it keeps working while the host is stopping (when the Dispatcher closes
				// the consumer connection) and resolving it never blocks. The container disposes it.
				.AddSingleton<IPublisher>(provider => Publisher.WithLazyConnection(
					cancellationToken => ConnectionFactoryBuilder.CreateConnectionAsync(configuration, ConnectionFactoryBuilder.PublisherSuffix, cancellationToken),
					provider.GetRequiredService<ISerializer>(),
					provider.GetRequiredService<ILogger<Publisher>>(),
					configuration))
				.AddHostedService<Dispatcher>();

		/// <summary>
		/// Builds one consumer per registered <see cref="IHandler{TMessage}"/> — including factory and
		/// instance registrations, and several handlers for the same message type.
		/// </summary>
		private static List<Consumer> CreateConsumers(IServiceCollection services, IServiceProvider provider, Configuration configuration)
		{
			var connection = provider.GetRequiredService<IConnection>();
			var serializer = provider.GetRequiredService<ISerializer>();
			var logger = provider.GetRequiredService<ILogger<Consumer>>();

			// Keyed descriptors are skipped: they are not part of GetServices, and reading
			// ImplementationType on a keyed descriptor throws.
			var handlerServiceTypes = services
				.Where(d => !d.IsKeyedService && IsClosedHandlerInterface(d.ServiceType))
				.Select(d => d.ServiceType)
				.Distinct();

			var consumers = new List<Consumer>();
			foreach (var serviceType in handlerServiceTypes)
			{
				var messageType = serviceType.GetGenericArguments()[0];

				// GetServices returns every registration in order. GetRequiredService would return
				// only the last one, so two handlers for one message type both ran the same instance.
				foreach (var handler in provider.GetServices(serviceType))
				{
					var handlerType = handler!.GetType();
					var attribute = handlerType.GetCustomAttribute<HandlerAttribute>()
						?? throw new InvalidOperationException(
							$"Handler '{handlerType.Name}' is registered as {serviceType.Name} but is not decorated with " +
							$"[{nameof(HandlerAttribute)}], so DouglasMQ does not know which queue it consumes.");

					consumers.Add(Consumer.Create(connection, serializer, logger, configuration, attribute, handler, messageType));
				}
			}

			return consumers;
		}

		private static bool IsClosedHandlerInterface(Type type) =>
			type.IsGenericType &&
			!type.ContainsGenericParameters &&
			type.GetGenericTypeDefinition() == typeof(IHandler<>);
	}
}
