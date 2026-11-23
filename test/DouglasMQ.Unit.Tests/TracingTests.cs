using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// One span and one log scope per handling attempt, joined to the sender's trace.
	/// </summary>
	public sealed class TracingTests : IDisposable
	{
		private readonly string _queue = $"tracing.{Guid.NewGuid():N}";
		private readonly ConcurrentQueue<Activity> _stopped = new();
		private readonly ActivityListener _listener;

		public TracingTests()
		{
			_listener = new ActivityListener
			{
				ShouldListenTo = source => source.Name == DouglasMQInstrumentation.ActivitySourceName,
				Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
				ActivityStopped = activity =>
				{
					if (Equals(activity.GetTagItem("messaging.consumer.group.name"), _queue))
					{
						_stopped.Enqueue(activity);
					}
				},
			};
			ActivitySource.AddActivityListener(_listener);
		}

		public void Dispose() => _listener.Dispose();

		private DeliveryProcessor CreateProcessor(object handler, ILogger? logger = null, int retryCount = 0)
			=> new(
				new SystemTextJsonSerializer(),
				HandlerInvoker.Create(handler),
				new RetryPolicy(retryCount, 0),
				logger ?? NullLogger.Instance,
				new HandlerAttribute(ExchangeType.topic, "tracing.exchange", _queue, "tracing.#"));

		private static ReadOnlyMemory<byte> Body(Message message) => Encoding.UTF8.GetBytes(new SystemTextJsonSerializer().Serialize(message));

		private static DeliveryInfo Delivery(IDictionary<string, object?>? headers = null) => new("tracing.exchange", "tracing.created", headers);

		[Fact]
		public async Task EachAttempt_GetsAConsumerSpanWithMessagingTags()
		{
			var message = new Message();
			var handler = new Handler { FailuresBeforeSuccess = 1 };

			var outcome = await CreateProcessor(handler, retryCount: 2).ProcessAsync(Body(message), Delivery(), CancellationToken.None);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			var spans = _stopped.ToList();
			Assert.Equal(2, spans.Count);
			Assert.All(spans, span =>
			{
				Assert.Equal($"{_queue} process", span.DisplayName);
				Assert.Equal(ActivityKind.Consumer, span.Kind);
				Assert.Equal("rabbitmq", span.GetTagItem("messaging.system"));
				Assert.Equal("process", span.GetTagItem("messaging.operation.type"));
				Assert.Equal("tracing.exchange", span.GetTagItem("messaging.destination.name"));
				Assert.Equal("tracing.created", span.GetTagItem("messaging.rabbitmq.destination.routing_key"));
				Assert.Equal(message.MessageId.ToString(), span.GetTagItem("messaging.message.id"));
			});

			var failed = spans[0];
			Assert.Equal(ActivityStatusCode.Error, failed.Status);
			Assert.Contains(failed.Events, e => e.Name == "exception");
			Assert.Equal(ActivityStatusCode.Unset, spans[1].Status);
		}

		[Fact]
		public async Task TraceparentHeader_ParentsTheSpan()
		{
			var traceId = ActivityTraceId.CreateRandom();
			var parentSpanId = ActivitySpanId.CreateRandom();
			var headers = new Dictionary<string, object?>
			{
				// RabbitMQ.Client delivers string headers as UTF-8 bytes.
				["traceparent"] = Encoding.UTF8.GetBytes($"00-{traceId}-{parentSpanId}-01"),
			};

			await CreateProcessor(new Handler()).ProcessAsync(Body(new Message()), Delivery(headers), CancellationToken.None);

			var span = Assert.Single(_stopped);
			Assert.Equal(traceId, span.TraceId);
			Assert.Equal(parentSpanId, span.ParentSpanId);
		}

		[Fact]
		public async Task WithoutTraceparent_TheMessageTraceIdIsTheTrace()
		{
			var message = new Message();

			await CreateProcessor(new Handler()).ProcessAsync(Body(message), Delivery(), CancellationToken.None);

			var span = Assert.Single(_stopped);
			Assert.Equal(message.TraceId.ToString("N"), span.TraceId.ToHexString());
		}

		[Fact]
		public async Task HandlerLogs_CarryMessageIdAndTraceIdScope()
		{
			var logger = new RecordingLogger();
			var message = new Message();
			var handler = new Handler { Logger = logger };

			await CreateProcessor(handler, logger).ProcessAsync(Body(message), Delivery(), CancellationToken.None);

			var entry = Assert.Single(logger.Entries, e => e.Message == "handling");
			var scope = Assert.IsAssignableFrom<IEnumerable<KeyValuePair<string, object>>>(Assert.Single(entry.Scopes)).ToDictionary(kv => kv.Key, kv => kv.Value);
			Assert.Equal(message.MessageId, scope["MessageId"]);
			Assert.Equal(message.TraceId, scope["TraceId"]);
		}

		public sealed class Message : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();

			public Guid TraceId { get; set; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.topic, "tracing.exchange", "tracing.queue", "tracing.#")]
		private sealed class Handler : IHandler<Message>
		{
			public int FailuresBeforeSuccess { get; set; }

			public ILogger? Logger { get; init; }

			public Task HandleAsync(Message message)
			{
				Logger?.LogInformation("handling");
				if (FailuresBeforeSuccess-- > 0)
				{
					throw new InvalidOperationException("transient");
				}

				return Task.CompletedTask;
			}
		}
	}
}
