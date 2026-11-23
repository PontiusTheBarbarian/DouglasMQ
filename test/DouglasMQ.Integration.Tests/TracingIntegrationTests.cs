using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class TracingIntegrationTests : IDisposable
	{
		private readonly RabbitMqFixture _fixture;
		private readonly ConcurrentQueue<Activity> _activities = new();
		private readonly ActivityListener _listener;

		public TracingIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
			_listener = new ActivityListener
			{
				ShouldListenTo = source => source.Name == DouglasMQInstrumentation.ActivitySourceName,
				Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
				ActivityStopped = activity => _activities.Enqueue(activity),
			};
			ActivitySource.AddActivityListener(_listener);
		}

		public void Dispose() => _listener.Dispose();

		[Fact]
		public async Task ProcessSpan_SharesTraceWithPublishSpan()
		{
			var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
			var builder = _fixture.CreateBuilder().AddHandler(new TracedEventHandler(() => received.TrySetResult()));
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			await publisher.PublishAsync(new TracedEvent());
			await received.Task.WaitAsync(TimeSpan.FromSeconds(10));

			// The process span stops just after the handler returns and the message is acked.
			var deadline = DateTime.UtcNow.AddSeconds(5);
			Activity? process = null;
			while (process is null && DateTime.UtcNow < deadline)
			{
				process = _activities.FirstOrDefault(a => a.OperationName == "process tracing.queue");
				if (process is null)
				{
					await Task.Delay(50);
				}
			}

			var publish = Assert.Single(_activities, a => a.OperationName == "publish tracing.exchange");
			Assert.NotNull(process);
			Assert.Equal(publish.TraceId, process!.TraceId);
			Assert.Equal("ack", process.GetTagItem("douglasmq.outcome"));
		}

		[Message("tracing.exchange", "traced.key")]
		public class TracedEvent : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();
			public Guid TraceId { get; set; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "tracing.exchange", "tracing.queue", "traced.key")]
		public class TracedEventHandler : IHandler<TracedEvent>
		{
			private readonly Action _onReceived;
			public TracedEventHandler(Action onReceived) => _onReceived = onReceived;
			public Task HandleAsync(TracedEvent message) { _onReceived(); return Task.CompletedTask; }
		}
	}
}
