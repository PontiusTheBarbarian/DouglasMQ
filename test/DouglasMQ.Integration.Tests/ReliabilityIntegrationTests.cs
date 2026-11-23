using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// Non-retryable failures, graceful and overlapping shutdown, and trace propagation against a real broker.
	/// </summary>
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class ReliabilityIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public ReliabilityIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		private async Task<(IConnection Connection, IChannel Channel)> OpenAdminChannelAsync()
		{
			var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			var channel = await connection.CreateChannelAsync();
			return (connection, channel);
		}

		[Fact]
		public async Task NonRetryableException_RunsHandlerOnce_AndDeadLettersWithoutWaitingForRetries()
		{
			var handler = new RejectingHandler();
			var (connection, admin) = await OpenAdminChannelAsync();
			await using var _ = connection;
			await using var __ = admin;

			var builder = _fixture.CreateBuilder()
				.Configure(c =>
				{
					c.RetryCount = 3;
					c.RetryDelayMs = 5_000; // a retry would take at least 5 s
					c.PublisherConfirms = true;
				})
				.AddHandler(handler);
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			var message = new RejectedCommand();
			var deadLettered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
			var dlxConsumer = new AsyncEventingBasicConsumer(admin);
			dlxConsumer.ReceivedAsync += (_, args) =>
			{
				if (args.BasicProperties.MessageId == message.MessageId.ToString())
				{
					deadLettered.TrySetResult();
				}

				return Task.CompletedTask;
			};
			await admin.BasicConsumeAsync("dlx.queue", autoAck: true, consumer: dlxConsumer);

			var stopwatch = Stopwatch.StartNew();
			await publisher.SendAsync(message);
			await deadLettered.Task.WaitAsync(TimeSpan.FromSeconds(10));

			Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(2), $"Dead-lettering took {stopwatch.Elapsed}.");
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task Stop_WhileHandlerRuns_LetsItFinishAndAckOnce_WithoutRedelivery()
		{
			const string queue = "reliability.slow.queue";
			var handler = new SlowHandler();
			var (connection, admin) = await OpenAdminChannelAsync();
			await using var _ = connection;
			await using var __ = admin;

			var builder = _fixture.CreateBuilder().Configure(c => c.PublisherConfirms = true).AddHandler(handler);
			var consumers = await builder.StartConsumersAsync();
			await admin.QueuePurgeAsync(queue);
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			await publisher.PublishAsync(new SlowEvent());
			await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));

			await consumers.DisposeAsync(); // stops while the 500 ms handler is running

			Assert.Equal(1, handler.Completed);
			Assert.Equal(1, handler.Calls);

			// Unacknowledged deliveries are requeued when the channel closes; an acked one is gone.
			Assert.Equal(0u, await admin.MessageCountAsync(queue));
		}

		[Fact]
		public async Task HostStop_CalledConcurrentlyTwice_PlusDispose_ShutsDownCleanly()
		{
			var logger = new CollectingLoggerProvider();
			var builder = Host.CreateApplicationBuilder();
			builder.Logging.ClearProviders().AddProvider(logger);
			builder.Services.AddSingleton<ISerializer, JsonSerializer>();
			builder.Services.AddDouglasMQ((config, services) =>
			{
				config.HostName = _fixture.HostName;
				config.Port = _fixture.Port;
				services.AddConsumer<SlowEvent, SlowHandler>();
				services.AddConsumer<RejectedCommand, RejectingHandler>();
			});

			var host = builder.Build();
			await host.StartAsync();
			await RabbitMqFixture.WaitForConsumersAsync(host.Services);
			var status = host.Services.GetRequiredService<IDouglasMQStatus>();

			// What app.Run() plus whoever fired the stopping token do, followed by container disposal.
			var hosted = host.Services.GetServices<IHostedService>().ToList();
			await Task.WhenAll(hosted.SelectMany(h => new[] { h.StopAsync(CancellationToken.None), h.StopAsync(CancellationToken.None) }));
			await ((IAsyncDisposable)host).DisposeAsync();

			Assert.False(status.ConsumersRunning);
			Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Warning);
		}

		[Fact]
		public async Task ProcessSpan_JoinsThePublishersTrace()
		{
			var processed = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
			using var listener = new ActivityListener
			{
				ShouldListenTo = source => source.Name == DouglasMQInstrumentation.ActivitySourceName || source.Name == "TestApp",
				Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
				ActivityStopped = activity =>
				{
					if (activity.DisplayName == "reliability.traced.queue process")
					{
						processed.TrySetResult(activity);
					}
				},
			};
			ActivitySource.AddActivityListener(listener);
			using var appSource = new ActivitySource("TestApp");

			var builder = _fixture.CreateBuilder().AddHandler(new TracedHandler());
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			ActivityTraceId requestTrace;
			using (var request = appSource.StartActivity("request"))
			{
				requestTrace = request!.TraceId;
				await publisher.PublishAsync(new TracedEvent());
			}

			var span = await processed.Task.WaitAsync(TimeSpan.FromSeconds(10));
			Assert.Equal(requestTrace, span.TraceId);
			Assert.Equal(ActivityKind.Consumer, span.Kind);
		}

		// --- Test doubles ---

		[Message("reliability.exchange", "rejected.key")]
		public sealed class RejectedCommand : ICommand
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "reliability.exchange", "reliability.rejected.queue", "rejected.key")]
		public sealed class RejectingHandler : IHandler<RejectedCommand>
		{
			private int _attempts;

			public int Attempts => _attempts;

			public Task HandleAsync(RejectedCommand message)
			{
				Interlocked.Increment(ref _attempts);
				throw new NonRetryableException("The command is invalid.");
			}
		}

		[Message("reliability.exchange", "slow.key")]
		public sealed class SlowEvent : IEvent
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "reliability.exchange", "reliability.slow.queue", "slow.key")]
		public sealed class SlowHandler : IHandler<SlowEvent>
		{
			private int _calls;
			private int _completed;

			public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

			public int Calls => _calls;

			public int Completed => _completed;

			public async Task HandleAsync(SlowEvent message)
			{
				Interlocked.Increment(ref _calls);
				Started.TrySetResult();
				await Task.Delay(500); // e.g. a database insert
				Interlocked.Increment(ref _completed);
			}
		}

		[Message("reliability.exchange", "traced.key")]
		public sealed class TracedEvent : IEvent
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "reliability.exchange", "reliability.traced.queue", "traced.key")]
		public sealed class TracedHandler : IHandler<TracedEvent>
		{
			public Task HandleAsync(TracedEvent message) => Task.CompletedTask;
		}

		private sealed class CollectingLoggerProvider : ILoggerProvider, ILogger
		{
			public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

			public ILogger CreateLogger(string categoryName) => this;

			public IDisposable? BeginScope<TState>(TState state)
				where TState : notnull
				=> null;

			public bool IsEnabled(LogLevel logLevel) => true;

			public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
				=> Entries.Enqueue((logLevel, formatter(state, exception) + (exception is null ? string.Empty : " " + exception)));

			public void Dispose()
			{
			}
		}
	}
}
