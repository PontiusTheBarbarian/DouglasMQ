using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class ServiceRegistrationTests
	{
		/// <summary>
		/// Registers DouglasMQ with a mocked consumer connection so no broker is needed.
		/// </summary>
		private static ServiceCollection CreateServices(Action<IServiceCollection> registerHandlers)
		{
			var services = new ServiceCollection();
			services.AddLogging();
			services.AddSingleton<ISerializer, Serializer>();
			services.AddDouglasMQ((_, s) => registerHandlers(s));
			services.AddSingleton(new Mock<IConnection>().Object); // overrides the real connection
			return services;
		}

		private static List<Consumer> ResolveConsumers(ServiceCollection services) =>
			services.BuildServiceProvider().GetRequiredService<List<Consumer>>();

		[Fact]
		public void TwoHandlersForSameMessageType_EachGetTheirOwnConsumer()
		{
			// Previously both consumers resolved the last-registered handler.
			var services = CreateServices(s =>
			{
				s.AddConsumer<DiMessage, QueueAHandler>();
				s.AddConsumer<DiMessage, QueueBHandler>();
			});

			var consumers = ResolveConsumers(services);

			Assert.Equal(
				new[] { (nameof(QueueAHandler), "di.queue.a"), (nameof(QueueBHandler), "di.queue.b") },
				consumers.Select(c => (c.HandlerName, c.QueueName)).OrderBy(x => x.QueueName));
		}

		[Fact]
		public void FactoryAndInstanceRegistrations_AreConsumed()
		{
			// Previously only registrations with an ImplementationType were found.
			var services = CreateServices(s =>
			{
				s.AddSingleton<IHandler<DiMessage>>(_ => new QueueAHandler());
				s.AddSingleton<IHandler<DiMessage>>(new QueueBHandler());
			});

			var consumers = ResolveConsumers(services);

			Assert.Equal(new[] { "di.queue.a", "di.queue.b" }, consumers.Select(c => c.QueueName).OrderBy(q => q));
		}

		[Fact]
		public void KeyedRegistrations_DoNotBreakConsumerDiscovery()
		{
			// Reading ImplementationType on a keyed descriptor throws.
			var services = CreateServices(s =>
			{
				s.AddKeyedSingleton<IHandler<DiMessage>, QueueAHandler>("some-key");
				s.AddConsumer<DiMessage, QueueBHandler>();
			});

			var consumer = Assert.Single(ResolveConsumers(services));
			Assert.Equal("di.queue.b", consumer.QueueName);
		}

		[Fact]
		public void HandlerWithoutAttribute_FailsWithClearError()
		{
			var services = CreateServices(s => s.AddSingleton<IHandler<DiMessage>, UndecoratedHandler>());

			var ex = Assert.Throws<InvalidOperationException>(() => ResolveConsumers(services));
			Assert.Contains(nameof(UndecoratedHandler), ex.Message);
		}

		[Fact]
		public void AutoRegisterConsumers_Twice_DoesNotDuplicateHandlers()
		{
			var once = new ServiceCollection().AutoRegisterConsumers(typeof(ServiceRegistrationTests)).Count;
			var twice = new ServiceCollection()
				.AutoRegisterConsumers(typeof(ServiceRegistrationTests))
				.AutoRegisterConsumers(typeof(ServiceRegistrationTests))
				.Count;

			Assert.True(once > 0);
			Assert.Equal(once, twice);
		}

		[Fact]
		public void Publisher_IsResolvableWithoutConnecting()
		{
			// The publisher has its own lazily-opened connection; resolving it must not block on I/O
			// (no broker is running here, so an eager connect would throw).
			var services = CreateServices(_ => { });

			var publisher = services.BuildServiceProvider().GetRequiredService<IPublisher>();

			Assert.IsType<Publisher>(publisher);
		}

		[Fact]
		public void HandlerDiscovery_SkipsAbstractAndOpenGenericTypes()
		{
			var found = HandlerDiscovery.FindHandlerTypes(typeof(ServiceRegistrationTests).Assembly).ToList();

			Assert.Contains(typeof(DerivedHandler), found);
			Assert.DoesNotContain(typeof(AbstractHandlerBase), found);
			Assert.DoesNotContain(typeof(GenericHandler<>), found);
		}

		// --- Test doubles ---

		public sealed class DiMessage : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "di.exchange", "di.queue.a", "di.key")]
		public sealed class QueueAHandler : IHandler<DiMessage>
		{
			public Task HandleAsync(DiMessage message) => Task.CompletedTask;
		}

		[Handler(ExchangeType.direct, "di.exchange", "di.queue.b", "di.key")]
		public sealed class QueueBHandler : IHandler<DiMessage>
		{
			public Task HandleAsync(DiMessage message) => Task.CompletedTask;
		}

		public sealed class UndecoratedHandler : IHandler<DiMessage>
		{
			public Task HandleAsync(DiMessage message) => Task.CompletedTask;
		}

		[Handler(ExchangeType.direct, "di.exchange", "di.queue.base", "di.key")]
		public abstract class AbstractHandlerBase : IHandler<DiMessage>
		{
			public Task HandleAsync(DiMessage message) => Task.CompletedTask;
		}

		/// <summary>Inherits [Handler] from its abstract base.</summary>
		public sealed class DerivedHandler : AbstractHandlerBase
		{
		}

		[Handler(ExchangeType.direct, "di.exchange", "di.queue.generic", "di.key")]
		public sealed class GenericHandler<T> : IHandler<DiMessage>
		{
			public Task HandleAsync(DiMessage message) => Task.CompletedTask;
		}

		private sealed class Serializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => null;

			public string Serialize<T>(T data) => "{}";
		}
	}
}
