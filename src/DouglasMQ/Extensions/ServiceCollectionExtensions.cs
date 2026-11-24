using System;
using System.Collections.Generic;
using System.Linq;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace DouglasMQ
{
	/// <summary>
	/// Extension methods for registering DouglasMQ services with Microsoft.Extensions.DependencyInjection.
	/// </summary>
	public static class ServiceCollectionExtensions
	{
		private const string DouglasMQ = nameof(DouglasMQ);

		/// <summary>
		/// Registers DouglasMQ services using a configuration delegate. Uses
		/// <see cref="SystemTextJsonSerializer"/> unless another <see cref="ISerializer"/> is registered
		/// (e.g. via <see cref="AddSerializer"/>), before or after this call.
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
		/// then registers each with <paramref name="lifetime"/>.
		/// </summary>
		/// <param name="services">The service collection.</param>
		/// <param name="assemblyMark">A type in the assembly to scan.</param>
		/// <param name="lifetime">
		/// The handlers' lifetime. With <see cref="ServiceLifetime.Scoped"/> (the default) or
		/// <see cref="ServiceLifetime.Transient"/>, each message is handled by a new handler instance
		/// created in its own scope, so handlers can depend on scoped services such as a <c>DbContext</c>.
		/// With <see cref="ServiceLifetime.Singleton"/>, one instance handles every message.
		/// </param>
		/// <exception cref="InvalidOperationException">
		/// A decorated handler implements more than one <see cref="IHandler{TMessage}"/>. A handler
		/// class maps to a single queue, so each message type needs its own handler class (or an
		/// explicit <see cref="AddConsumer{TMessage, TImp}"/> registration).
		/// </exception>
		public static IServiceCollection AutoRegisterConsumers(this IServiceCollection services, Type assemblyMark,
			ServiceLifetime lifetime = ServiceLifetime.Scoped)
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
				services.TryAddEnumerable(ServiceDescriptor.Describe(typeof(IHandler<>).MakeGenericType(messageTypes[0]), handlerType, lifetime));
			}

			return services;
		}

		/// <summary>
		/// Manually registers a single handler.
		/// </summary>
		/// <param name="services">The service collection.</param>
		/// <param name="lifetime">
		/// The handler's lifetime. With <see cref="ServiceLifetime.Scoped"/> (the default) or
		/// <see cref="ServiceLifetime.Transient"/>, each message is handled by a new handler instance
		/// created in its own scope. With <see cref="ServiceLifetime.Singleton"/>, one instance handles
		/// every message.
		/// </param>
		public static IServiceCollection AddConsumer<TMessage, TImp>(this IServiceCollection services,
			ServiceLifetime lifetime = ServiceLifetime.Scoped)
			where TMessage : class, IMessage
			where TImp : class, IHandler<TMessage>
		{
			services.TryAddEnumerable(ServiceDescriptor.Describe(typeof(IHandler<TMessage>), typeof(TImp), lifetime));
			return services;
		}

		/// <summary>
		/// Registers only the publisher: an <see cref="IPublisher"/> that opens its own connection on the
		/// first publish, using the <c>DouglasMQ</c> section of <paramref name="configuration"/>. Starts no
		/// consumers and declares nothing, so it suits an API that sends commands to a separate worker.
		/// </summary>
		/// <param name="services">The service collection.</param>
		/// <param name="configuration">Configuration containing a <c>DouglasMQ</c> section.</param>
		/// <param name="serializer">
		/// The <see cref="ISerializer"/> implementation, or <c>null</c> for <see cref="SystemTextJsonSerializer"/>
		/// unless another serializer is registered.
		/// </param>
		/// <remarks>
		/// Building the container and starting the host never touch the network, so an unreachable broker
		/// does not stop the app starting; the first publish fails instead, and the next one tries again.
		/// Use <see cref="DouglasMQHealthCheckExtensions.AddDouglasMQPublisherCheck"/> to report the
		/// connection on a readiness probe. Calling this as well as <c>AddDouglasMQ</c> is harmless: both
		/// register the same single publisher, configured by whichever call comes first.
		/// </remarks>
		public static IServiceCollection AddDouglasMQPublisher(this IServiceCollection services,
			IConfiguration configuration,
			Type? serializer = null)
		{
			ArgumentNullException.ThrowIfNull(configuration);

			var config = new Configuration();
			configuration.GetSection(DouglasMQ).Bind(config);

			if (serializer is not null)
			{
				services.AddSerializer(serializer);
			}

			return services.AddDouglasMQPublisher(config);
		}

		/// <summary>
		/// Registers only the publisher, configured in code. See
		/// <see cref="AddDouglasMQPublisher(IServiceCollection, IConfiguration, Type)"/>.
		/// </summary>
		public static IServiceCollection AddDouglasMQPublisher(this IServiceCollection services,
			Action<Configuration> configure)
		{
			ArgumentNullException.ThrowIfNull(configure);

			var config = new Configuration();
			configure(config);
			return services.AddDouglasMQPublisher(config);
		}

		private static IServiceCollection AddDouglasMQPublisher(this IServiceCollection services, Configuration configuration)
		{
			services.TryAddSingleton(configuration);
			services.TryAddSingleton<ISerializer, SystemTextJsonSerializer>();
			return services.AddLazyPublisher(configuration);
		}

		/// <summary>
		/// The publisher gets its own lazily-opened connection rather than sharing the consumer connection,
		/// so it keeps working while the host is stopping (when the Dispatcher closes the consumer
		/// connection) and resolving it never blocks. The container disposes it. TryAdd, so registering
		/// both the publisher and the consumers yields one publisher.
		/// </summary>
		private static IServiceCollection AddLazyPublisher(this IServiceCollection services, Configuration configuration)
		{
			services.TryAddSingleton(provider => Publisher.Create(
				configuration,
				provider.GetRequiredService<ISerializer>(),
				provider.GetRequiredService<ILogger<Publisher>>()));
			services.TryAddSingleton<IPublisher>(provider => provider.GetRequiredService<Publisher>());
			return services;
		}

		/// <summary>
		/// Registers a serializer implementation.
		/// </summary>
		public static IServiceCollection AddSerializer(this IServiceCollection services, Type serializer)
			=> services.AddSingleton(typeof(ISerializer), serializer);

		private static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			Configuration configuration)
		{
			services
				.AddSingleton(configuration)
				// Default serializer unless the app registered its own (before or after this call:
				// AddSerializer uses AddSingleton, and the last registration wins).
				.TryAddSingletonFluent<ISerializer, SystemTextJsonSerializer>()
				.AddSingleton<IReadOnlyList<HandlerRegistration>>(provider => CreateRegistrations(services, provider))
				.AddLazyPublisher(configuration)
				// The Dispatcher opens (and owns) the consumer connection in the background once the host
				// starts it, retrying while the broker is unreachable, so neither building the container nor
				// starting the host blocks on (or fails because of) the network.
				.AddSingleton(provider => Dispatcher.Create(
					provider.GetRequiredService<IReadOnlyList<HandlerRegistration>>(),
					provider.GetRequiredService<ISerializer>(),
					configuration,
					provider.GetRequiredService<ILoggerFactory>()))
				.AddSingleton<IDouglasMQStatus>(provider => provider.GetRequiredService<Dispatcher>())
				.AddHostedService(provider => provider.GetRequiredService<Dispatcher>());

			return services;
		}

		/// <summary>
		/// Builds one <see cref="HandlerRegistration"/> per registered <see cref="IHandler{TMessage}"/> —
		/// including factory and instance registrations, and several handlers for the same message type.
		/// </summary>
		/// <remarks>
		/// Singleton handlers are resolved once and reused for every message. Scoped and transient
		/// handlers get a new dependency injection scope per handling attempt (so a retry never reuses
		/// a scoped service, such as a <c>DbContext</c>, left in a bad state by the failed attempt), and
		/// a new handler instance created in it. Each one is also created once here, so a handler that
		/// cannot be constructed fails at startup rather than on its first message.
		/// </remarks>
		private static List<HandlerRegistration> CreateRegistrations(IServiceCollection services, IServiceProvider provider)
		{
			var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();

			// Keyed descriptors are skipped: they are not part of GetServices, and reading
			// ImplementationType on a keyed descriptor throws.
			var handlerDescriptors = services
				.Where(d => !d.IsKeyedService && IsClosedHandlerInterface(d.ServiceType))
				.GroupBy(d => d.ServiceType);

			var registrations = new List<HandlerRegistration>();
			foreach (var group in handlerDescriptors)
			{
				var serviceType = group.Key;
				var messageType = serviceType.GetGenericArguments()[0];
				var descriptors = group.ToList();

				// A scope for resolving singletons (a singleton resolved from the root provider would
				// bypass scope validation) and for trying out the other handlers once at startup.
				var startupScope = scopeFactory.CreateAsyncScope();
				try
				{
					var singletons = descriptors.Any(d => d.Lifetime == ServiceLifetime.Singleton)
						? GetHandlerRegistrations(startupScope.ServiceProvider, serviceType, descriptors.Count)
						: null;

					for (var i = 0; i < descriptors.Count; i++)
					{
						HandlerInvoker invoker;
						if (descriptors[i].Lifetime == ServiceLifetime.Singleton)
						{
							invoker = HandlerInvoker.Create(singletons![i]!, messageType);
						}
						else
						{
							var activate = CreateActivator(descriptors[i]);
							var handlerType = GetHandlerTypeByActivating(activate, startupScope.ServiceProvider);
							invoker = HandlerInvoker.CreateScoped(scopeFactory, handlerType, messageType, activate);
						}

						registrations.Add(HandlerRegistration.For(invoker));
					}
				}
				finally
				{
					// Runs once at startup, from a synchronous factory.
					startupScope.DisposeAsync().AsTask().GetAwaiter().GetResult();
				}
			}

			return registrations;
		}

		/// <summary>
		/// Resolves every registration of <paramref name="serviceType"/>, in registration order, so they
		/// line up with the descriptors.
		/// </summary>
		private static List<object?> GetHandlerRegistrations(IServiceProvider provider, Type serviceType, int expectedCount)
		{
			// GetServices returns every registration in order. GetRequiredService would return
			// only the last one, so two handlers for one message type both ran the same instance.
			var handlers = provider.GetServices(serviceType).ToList();
			if (handlers.Count != expectedCount)
			{
				throw new InvalidOperationException(
					$"Expected {expectedCount} registration(s) of {serviceType.Name} but the container returned {handlers.Count}. " +
					$"Open generic {nameof(IHandler<IMessage>)}<> registrations are not supported; register each handler for its message type.");
			}

			return handlers;
		}

		/// <summary>
		/// Creates a scoped or transient handler from a scope's provider, the way the container would
		/// for that one registration (resolving every registration would also create its siblings).
		/// </summary>
		private static Func<IServiceProvider, object> CreateActivator(ServiceDescriptor descriptor)
		{
			if (descriptor.ImplementationFactory is { } factory)
			{
				return factory;
			}

			var implementationType = descriptor.ImplementationType!;
			var create = ActivatorUtilities.CreateFactory(implementationType, Type.EmptyTypes);
			return provider => create(provider, null);
		}

		private static Type GetHandlerTypeByActivating(Func<IServiceProvider, object> activate, IServiceProvider provider)
		{
			var handler = activate(provider);
			var handlerType = handler.GetType();

			HandlerInvoker.DisposeHandlerAsync(handler).AsTask().GetAwaiter().GetResult();

			return handlerType;
		}

		private static IServiceCollection TryAddSingletonFluent<TService, TImplementation>(this IServiceCollection services)
			where TService : class
			where TImplementation : class, TService
		{
			services.TryAddSingleton<TService, TImplementation>();
			return services;
		}

		private static bool IsClosedHandlerInterface(Type type) =>
			type.IsGenericType &&
			!type.ContainsGenericParameters &&
			type.GetGenericTypeDefinition() == typeof(IHandler<>);
	}
}
