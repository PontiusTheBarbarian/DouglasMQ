using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// Verifies that scoped and transient handlers get a new dependency injection scope per message.
	/// </summary>
	public class ScopedHandlerTests
	{
		private const ulong DeliveryTag = 7;

		private readonly Mock<IChannel> _channel = new();
		private readonly Tracker _tracker = new();

		public ScopedHandlerTests()
		{
			_channel.SetupGet(c => c.IsOpen).Returns(true);
		}

		private Consumer SingleConsumer(Action<IServiceCollection> register, int retryCount = 0)
		{
			var services = new ServiceCollection();
			services.AddLogging();
			services.AddSingleton(_tracker);
			services.AddScoped<ScopedDependency>();
			services.AddDouglasMQ((config, s) =>
			{
				config.RetryCount = retryCount;
				config.RetryDelayMs = 0;
				s.AddSingleton<ISerializer, SystemTextJsonSerializer>();
				register(s);
			});

			// Scope validation on, as in the Development environment: a scoped dependency captured by
			// a handler resolved from the root provider would throw here.
			var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
			var registration = Assert.Single(provider.GetRequiredService<IReadOnlyList<HandlerRegistration>>());
			return Consumer.Create(
				new Mock<IConnection>().Object,
				registration,
				provider.GetRequiredService<ISerializer>(),
				provider.GetRequiredService<Configuration>(),
				NullLogger.Instance);
		}

		private Task DeliverAsync(Consumer consumer) =>
			consumer.HandleDeliveryAsync(_channel.Object, new BasicDeliverEventArgs(
				consumerTag: "tag",
				deliveryTag: DeliveryTag,
				redelivered: false,
				exchange: "scoped.exchange",
				routingKey: "scoped.key",
				properties: new BasicProperties(),
				body: Encoding.UTF8.GetBytes("{}")));

		[Fact]
		public async Task ScopedHandler_GetsNewHandlerAndScopePerMessage()
		{
			var consumer = SingleConsumer(s => s.AddConsumer<ScopedMessage, ScopedHandler>());
			_tracker.Reset(); // ignore the instance created at startup to validate the handler

			await DeliverAsync(consumer);
			await DeliverAsync(consumer);

			Assert.Equal(2, _tracker.Handlers.Distinct().Count());
			Assert.Equal(2, _tracker.Dependencies.Distinct().Count());
			Assert.All(_tracker.Dependencies, d => Assert.True(d.Disposed));
			_channel.Verify(c => c.BasicAckAsync(DeliveryTag, false, It.IsAny<CancellationToken>()), Times.Exactly(2));
		}

		[Fact]
		public async Task ScopedHandler_IsDisposedAfterEachMessage()
		{
			var consumer = SingleConsumer(s => s.AddConsumer<ScopedMessage, ScopedHandler>());
			_tracker.Reset();

			await DeliverAsync(consumer);

			var handler = Assert.Single(_tracker.Handlers);
			Assert.True(handler.Disposed);
		}

		[Fact]
		public async Task Retry_GetsFreshScope()
		{
			// A failed attempt can leave a scoped service (e.g. a DbContext) in a bad state.
			_tracker.FailuresRemaining = 1;
			var consumer = SingleConsumer(s => s.AddConsumer<ScopedMessage, ScopedHandler>(), retryCount: 1);
			_tracker.Reset();

			await DeliverAsync(consumer);

			Assert.Equal(2, _tracker.Dependencies.Distinct().Count());
			_channel.Verify(c => c.BasicAckAsync(DeliveryTag, false, It.IsAny<CancellationToken>()), Times.Once);
		}

		[Fact]
		public async Task TransientAndFactoryRegistrations_AreScopedPerMessage()
		{
			var consumer = SingleConsumer(s => s.AddTransient<IHandler<ScopedMessage>>(sp =>
				new ScopedHandler(sp.GetRequiredService<ScopedDependency>(), sp.GetRequiredService<Tracker>())));
			_tracker.Reset();

			await DeliverAsync(consumer);
			await DeliverAsync(consumer);

			Assert.Equal(2, _tracker.Handlers.Distinct().Count());
			Assert.All(_tracker.Handlers, h => Assert.True(h.Disposed));
		}

		[Fact]
		public async Task SingletonHandler_IsReusedAcrossMessages()
		{
			var consumer = SingleConsumer(s => s.AddConsumer<ScopedMessage, SingletonHandler>(ServiceLifetime.Singleton));

			await DeliverAsync(consumer);
			await DeliverAsync(consumer);

			Assert.Single(_tracker.Singletons.Distinct());
			Assert.Equal(2, _tracker.Singletons.Count);
		}

		[Fact]
		public void SingletonHandlerWithScopedDependency_FailsAtStartup()
		{
			Assert.Throws<InvalidOperationException>(() =>
				SingleConsumer(s => s.AddConsumer<ScopedMessage, ScopedHandler>(ServiceLifetime.Singleton)));
		}

		[Fact]
		public void ScopedHandlerWithMissingDependency_FailsAtStartup()
		{
			var ex = Assert.Throws<InvalidOperationException>(() =>
				SingleConsumer(s => s.AddConsumer<ScopedMessage, HandlerWithMissingDependency>()));
			Assert.Contains(nameof(IMissingService), ex.Message);
		}

		[Fact]
		public void AddConsumer_DefaultsToScoped()
		{
			var descriptor = Assert.Single(new ServiceCollection().AddConsumer<ScopedMessage, ScopedHandler>());

			Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
		}

		[Fact]
		public void AutoRegisterConsumers_DefaultsToScoped_AndHonoursLifetime()
		{
			var scoped = new ServiceCollection().AutoRegisterConsumers(typeof(ScopedHandlerTests));
			var singleton = new ServiceCollection().AutoRegisterConsumers(typeof(ScopedHandlerTests), ServiceLifetime.Singleton);

			Assert.All(scoped, d => Assert.Equal(ServiceLifetime.Scoped, d.Lifetime));
			Assert.All(singleton, d => Assert.Equal(ServiceLifetime.Singleton, d.Lifetime));
		}

		// --- Test doubles ---

		public sealed class ScopedMessage : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();

			public Guid TraceId { get; set; } = Guid.NewGuid();
		}

		public sealed class Tracker
		{
			public ConcurrentQueue<ScopedHandler> HandlerQueue { get; } = new();

			public ConcurrentQueue<ScopedDependency> DependencyQueue { get; } = new();

			public ConcurrentQueue<SingletonHandler> SingletonQueue { get; } = new();

			public int FailuresRemaining { get; set; }

			public IReadOnlyList<ScopedHandler> Handlers => HandlerQueue.ToList();

			public IReadOnlyList<ScopedDependency> Dependencies => DependencyQueue.ToList();

			public IReadOnlyList<SingletonHandler> Singletons => SingletonQueue.ToList();

			public void Reset()
			{
				HandlerQueue.Clear();
				DependencyQueue.Clear();
				SingletonQueue.Clear();
			}
		}

		public sealed class ScopedDependency : IDisposable
		{
			public bool Disposed { get; private set; }

			public void Dispose() => Disposed = true;
		}

		public interface IMissingService
		{
		}

		[Handler(ExchangeType.direct, "scoped.exchange", "scoped.queue", "scoped.key")]
		public sealed class ScopedHandler : IHandler<ScopedMessage>, IDisposable
		{
			private readonly ScopedDependency _dependency;
			private readonly Tracker _tracker;

			public ScopedHandler(ScopedDependency dependency, Tracker tracker)
			{
				_dependency = dependency;
				_tracker = tracker;
			}

			public bool Disposed { get; private set; }

			public Task HandleAsync(ScopedMessage message)
			{
				Assert.False(_dependency.Disposed);
				_tracker.HandlerQueue.Enqueue(this);
				_tracker.DependencyQueue.Enqueue(_dependency);

				if (_tracker.FailuresRemaining > 0)
				{
					_tracker.FailuresRemaining--;
					throw new InvalidOperationException("Simulated failure.");
				}

				return Task.CompletedTask;
			}

			public void Dispose() => Disposed = true;
		}

		[Handler(ExchangeType.direct, "scoped.exchange", "singleton.queue", "scoped.key")]
		public sealed class SingletonHandler : IHandler<ScopedMessage>
		{
			private readonly Tracker _tracker;

			public SingletonHandler(Tracker tracker) => _tracker = tracker;

			public Task HandleAsync(ScopedMessage message)
			{
				_tracker.SingletonQueue.Enqueue(this);
				return Task.CompletedTask;
			}
		}

		[Handler(ExchangeType.direct, "scoped.exchange", "missing.queue", "scoped.key")]
		public sealed class HandlerWithMissingDependency : IHandler<ScopedMessage>
		{
			public HandlerWithMissingDependency(IMissingService service)
			{
			}

			public Task HandleAsync(ScopedMessage message) => Task.CompletedTask;
		}
	}
}
