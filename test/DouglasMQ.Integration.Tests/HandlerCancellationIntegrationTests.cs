using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// R7: a handler still running when the shutdown timeout runs out is cancelled; one that finishes in
	/// time is not. R9: the publisher-only registration and its health check against a real broker.
	/// </summary>
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class HandlerCancellationIntegrationTests
	{
		private const string Exchange = "cancellation.exchange";
		private const string HangingQueue = "cancellation.hanging.queue";
		private const string QuickQueue = "cancellation.quick.queue";

		private readonly RabbitMqFixture _fixture;

		public HandlerCancellationIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		private IHost BuildHost(TimeSpan shutdownTimeout, Action<IServiceCollection> handlers)
		{
			var builder = Host.CreateApplicationBuilder();
			builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = shutdownTimeout);
			builder.Services.AddSingleton<ISerializer, JsonSerializer>();
			builder.Services.AddSingleton<Probe>();
			builder.Services.AddDouglasMQ((config, services) =>
			{
				config.HostName = _fixture.HostName;
				config.Port = _fixture.Port;
				handlers(services);
			});
			return builder.Build();
		}

		[Fact]
		public async Task HostStop_CancelsAHandlerThatOutlivesTheShutdownTimeout_AndTheMessageIsRedelivered()
		{
			using var host = BuildHost(TimeSpan.FromSeconds(1), s => s.AddConsumer<HangingCommand, HangingHandler>());
			await host.StartAsync();
			await RabbitMqFixture.WaitForConsumersAsync(host.Services);
			var probe = host.Services.GetRequiredService<Probe>();

			await using (var publisher = (Publisher)await _fixture.CreateBuilder().BuildPublisherAsync())
			{
				await publisher.SendAsync(new HangingCommand());
			}

			await probe.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));

			var stopwatch = Stopwatch.StartNew();
			await host.StopAsync();

			await probe.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
			Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), $"Stopping took {stopwatch.Elapsed}.");

			// Not acked and not dead-lettered: the broker requeues it for the next consumer.
			await using var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			await using var admin = await connection.CreateChannelAsync();
			var deadline = DateTime.UtcNow.AddSeconds(10);
			while (await admin.MessageCountAsync(HangingQueue) != 1)
			{
				Assert.True(DateTime.UtcNow < deadline, "The cancelled delivery was not requeued.");
				await Task.Delay(25);
			}

			await admin.QueuePurgeAsync(HangingQueue);
		}

		[Fact]
		public async Task HostStop_DoesNotCancelAHandlerThatFinishesInTime()
		{
			using var host = BuildHost(TimeSpan.FromSeconds(10), s => s.AddConsumer<QuickCommand, QuickHandler>());
			await host.StartAsync();
			await RabbitMqFixture.WaitForConsumersAsync(host.Services);
			var probe = host.Services.GetRequiredService<Probe>();

			await using (var publisher = (Publisher)await _fixture.CreateBuilder().BuildPublisherAsync())
			{
				await publisher.SendAsync(new QuickCommand());
			}

			await probe.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
			await host.StopAsync();

			Assert.True(probe.Completed.Task.IsCompletedSuccessfully, "The handler should have finished before the host stopped.");
			Assert.False(await probe.Completed.Task, "The handler's token was cancelled although it finished in time.");
		}

		[Fact]
		public async Task PublisherCheck_IsHealthy_AgainstAReachableBroker()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQPublisher(c =>
			{
				c.HostName = _fixture.HostName;
				c.Port = _fixture.Port;
			});
			services.AddHealthChecks().AddDouglasMQPublisherCheck();
			await using var provider = services.BuildServiceProvider();

			var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

			Assert.Equal(HealthStatus.Healthy, report.Status);
		}

		public sealed class Probe
		{
			public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

			public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

			/// <summary>Completes with whether the handler's token was cancelled when it finished.</summary>
			public TaskCompletionSource<bool> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		[Message(Exchange, "hanging.key")]
		public sealed class HangingCommand : ICommand
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();
		}

		[Message(Exchange, "quick.key")]
		public sealed class QuickCommand : ICommand
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, Exchange, HangingQueue, "hanging.key")]
		public sealed class HangingHandler : IHandler<HangingCommand>
		{
			private readonly Probe _probe;

			public HangingHandler(Probe probe) => _probe = probe;

			public async Task HandleAsync(HangingCommand message, CancellationToken cancellationToken)
			{
				_probe.Started.TrySetResult();
				try
				{
					await Task.Delay(Timeout.Infinite, cancellationToken);
				}
				catch (OperationCanceledException)
				{
					_probe.Cancelled.TrySetResult();
					throw;
				}
			}
		}

		[Handler(ExchangeType.direct, Exchange, QuickQueue, "quick.key")]
		public sealed class QuickHandler : IHandler<QuickCommand>
		{
			private readonly Probe _probe;

			public QuickHandler(Probe probe) => _probe = probe;

			public async Task HandleAsync(QuickCommand message, CancellationToken cancellationToken)
			{
				_probe.Started.TrySetResult();
				await Task.Delay(300, CancellationToken.None);
				_probe.Completed.TrySetResult(cancellationToken.IsCancellationRequested);
			}
		}
	}
}
