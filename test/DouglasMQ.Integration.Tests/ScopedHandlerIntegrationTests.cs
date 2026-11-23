using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// With Microsoft DI, each message is handled in its own scope.
	/// </summary>
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class ScopedHandlerIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public ScopedHandlerIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task DiHost_HandlesEachMessageInItsOwnScope()
		{
			var recorder = new ScopeRecorder(expected: 2);

			// The Development environment turns on scope validation, so a scoped dependency
			// captured by a root-resolved handler would fail the host on start.
			var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
			builder.Services.AddSingleton<ISerializer, JsonSerializer>();
			builder.Services.AddSingleton(recorder);
			builder.Services.AddScoped<ScopedDependency>();
			builder.Services.AddDouglasMQ((config, services) =>
			{
				config.HostName = _fixture.HostName;
				config.Port = _fixture.Port;
				config.PublisherConfirms = true;
				services.AddConsumer<ScopedEvent, ScopedEventHandler>();
			});

			using var host = builder.Build();
			await host.StartAsync();
			try
			{
				var publisher = host.Services.GetRequiredService<IPublisher>();
				await publisher.PublishAsync(new ScopedEvent());
				await publisher.PublishAsync(new ScopedEvent());

				await recorder.Done.WaitAsync(TimeSpan.FromSeconds(10));
			}
			finally
			{
				await host.StopAsync();
			}

			// One scoped dependency per message. Disposal is covered deterministically by the unit tests.
			Assert.Equal(2, recorder.Dependencies.Distinct().Count());
		}

		[Message("scoped.integration.exchange", "scoped.key")]
		public sealed class ScopedEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		public sealed class ScopedDependency : IDisposable
		{
			public bool Disposed { get; private set; }

			public void Dispose() => Disposed = true;
		}

		public sealed class ScopeRecorder
		{
			private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
			private readonly int _expected;
			private int _count;

			public ScopeRecorder(int expected) => _expected = expected;

			public ConcurrentQueue<ScopedDependency> Dependencies { get; } = new();

			public Task Done => _done.Task;

			public void Record(ScopedDependency dependency)
			{
				Dependencies.Enqueue(dependency);
				if (Interlocked.Increment(ref _count) == _expected)
				{
					_done.TrySetResult();
				}
			}
		}

		[Handler(ExchangeType.direct, "scoped.integration.exchange", "scoped.integration.queue", "scoped.key")]
		public sealed class ScopedEventHandler : IHandler<ScopedEvent>
		{
			private readonly ScopedDependency _dependency;
			private readonly ScopeRecorder _recorder;

			public ScopedEventHandler(ScopedDependency dependency, ScopeRecorder recorder)
			{
				_dependency = dependency;
				_recorder = recorder;
			}

			public Task HandleAsync(ScopedEvent message)
			{
				Assert.False(_dependency.Disposed);
				_recorder.Record(_dependency);
				return Task.CompletedTask;
			}
		}
	}
}
