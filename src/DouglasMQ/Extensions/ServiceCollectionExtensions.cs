using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
		public static IServiceCollection AutoRegisterConsumers(this IServiceCollection services, Type assemblyMark)
		{
			const string HandleAsync = nameof(HandleAsync);

			var handlers = Assembly.GetAssembly(assemblyMark)!
				.GetTypes()
				.Where(i =>
					i.GetCustomAttribute<HandlerAttribute>() != null &&
					i.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IHandler<>)));

			foreach (var handler in handlers)
			{
				var parameter = handler.GetMethod(HandleAsync)!
					.GetParameters()
					.Single(p => p.ParameterType.GetCustomAttributes(typeof(MessageAttribute), true).Any());

				services.AddSingleton(typeof(IHandler<>).MakeGenericType(parameter.ParameterType), handler);
			}

			return services;
		}

		/// <summary>
		/// Manually registers a single handler.
		/// </summary>
		public static IServiceCollection AddConsumer<TMessage, TImp>(this IServiceCollection services)
			where TMessage : class, IMessage
			where TImp : class, IHandler<TMessage>
			=> services.AddSingleton<IHandler<TMessage>, TImp>();

		/// <summary>
		/// Registers a serializer implementation.
		/// </summary>
		public static IServiceCollection AddSerializer(this IServiceCollection services, Type serializer)
			=> services.AddSingleton(typeof(ISerializer), serializer);

		private static IServiceCollection AddDouglasMQ(this IServiceCollection services,
			Configuration configuration) => services
				.AddSingleton(configuration)
				// Async connection creation is deferred to the Dispatcher's StartAsync to avoid
				// blocking .Result calls during DI registration.
				.AddSingleton<IConnection>(provider =>
				{
					var builder = new DouglasMQBuilder();
					builder.Configure(c =>
					{
						c.Username = configuration.Username;
						c.Password = configuration.Password;
						c.VirtualHost = configuration.VirtualHost;
						c.ClientProvidedName = configuration.ClientProvidedName;
						c.HostName = configuration.HostName;
						c.UseSsl = configuration.UseSsl;
						c.SslCertificateThumbprint = configuration.SslCertificateThumbprint;
					});
					// Safe to block here: IConnection is resolved lazily from the first consumer/publisher
					// usage which occurs inside IHostedService.StartAsync (already on a thread pool thread).
					return builder.CreateConnectionAsync().GetAwaiter().GetResult();
				})
				.AddSingleton<List<Consumer>>(provider =>
				{
					var connection = provider.GetRequiredService<IConnection>();
					var serializer = provider.GetRequiredService<ISerializer>();
					var logger = provider.GetRequiredService<ILogger<Consumer>>();

					return services
						.Where(service =>
							service.ImplementationType?.GetCustomAttribute<HandlerAttribute>() != null &&
							service.ServiceType.IsGenericType &&
							service.ServiceType.GetGenericTypeDefinition() == typeof(IHandler<>))
						.Select(service =>
							Consumer.Create(
								connection,
								serializer,
								logger,
								configuration,
								service.ImplementationType!.GetCustomAttribute<HandlerAttribute>()!,
								provider.GetRequiredService(service.ServiceType)))
						.ToList();
				})
				.AddSingleton<IPublisher, Publisher>()
				.AddHostedService<Dispatcher>();
	}
}
