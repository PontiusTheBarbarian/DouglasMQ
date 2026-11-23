using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Diagnostics;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	[CollectionDefinition(Name, DisableParallelization = true)]
	public sealed class TelemetryCollection
	{
		public const string Name = "Telemetry";
	}

	[Collection(TelemetryCollection.Name)]
	public sealed class TelemetryTests : IDisposable
	{
		private const string Exchange = "telemetry.exchange";
		private const string Queue = "telemetry-queue";
		private const string RoutingKey = "telemetry.key";

		private readonly ConcurrentQueue<Activity> _activities = new();
		private readonly ConcurrentQueue<(string Instrument, double Value, Dictionary<string, object?> Tags)> _measurements = new();
		private readonly ActivityListener _activityListener;
		private readonly MeterListener _meterListener;

		public TelemetryTests()
		{
			_activityListener = new ActivityListener
			{
				ShouldListenTo = source => source.Name == DouglasMQInstrumentation.ActivitySourceName || source.Name == "Test",
				Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
				ActivityStopped = activity => _activities.Enqueue(activity),
			};
			ActivitySource.AddActivityListener(_activityListener);

			_meterListener = new MeterListener
			{
				InstrumentPublished = (instrument, listener) =>
				{
					if (instrument.Meter.Name == DouglasMQInstrumentation.MeterName)
					{
						listener.EnableMeasurementEvents(instrument);
					}
				},
			};
			_meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
			_meterListener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
			_meterListener.Start();
		}

		public void Dispose()
		{
			_activityListener.Dispose();
			_meterListener.Dispose();
		}

		// --- Publishing ---

		[Fact]
		public async Task PublishAsync_CreatesProducerSpanWithMessagingTags()
		{
			var (publisher, _) = CreatePublisher();
			var message = new TelemetryEvent();

			await publisher.PublishAsync(message);

			var span = Assert.Single(_activities, a => a.OperationName == $"publish {Exchange}");
			Assert.Equal(ActivityKind.Producer, span.Kind);
			Assert.Equal("rabbitmq", span.GetTagItem("messaging.system"));
			Assert.Equal("publish", span.GetTagItem("messaging.operation.name"));
			Assert.Equal(Exchange, span.GetTagItem("messaging.destination.name"));
			Assert.Equal(RoutingKey, span.GetTagItem("messaging.rabbitmq.destination.routing_key"));
			Assert.Equal(message.MessageId.ToString(), span.GetTagItem("messaging.message.id"));
			Assert.Equal(message.TraceId.ToString(), span.GetTagItem("messaging.message.conversation_id"));
			Assert.Equal(nameof(TelemetryEvent), span.GetTagItem("douglasmq.message.type"));
			Assert.Equal(ActivityStatusCode.Unset, span.Status);
		}

		[Fact]
		public async Task PublishAsync_InjectsTraceContextIntoHeaders()
		{
			var (publisher, channel) = CreatePublisher();
			BasicProperties? captured = null;
			channel.Setup(c => c.BasicPublishAsync(
					It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
					It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
				.Callback((string _, string _, bool _, BasicProperties props, ReadOnlyMemory<byte> _, CancellationToken _) => captured = props)
				.Returns(ValueTask.CompletedTask);

			var message = new TelemetryEvent();
			await publisher.PublishAsync(message);

			var span = Assert.Single(_activities, a => a.OperationName == $"publish {Exchange}");
			Assert.NotNull(captured);
			Assert.Equal(span.Id, captured!.Headers!["traceparent"]);
			Assert.Equal(message.MessageId.ToString(), captured.MessageId);
			Assert.Equal(message.TraceId.ToString(), captured.CorrelationId);
		}

		[Fact]
		public async Task PublishAsync_ParentsOnAmbientActivity()
		{
			var (publisher, _) = CreatePublisher();
			using var source = new ActivitySource("Test");

			string? parentSpanId;
			using (var parent = source.StartActivity("request"))
			{
				parentSpanId = parent!.SpanId.ToString();
				await publisher.PublishAsync(new TelemetryEvent());
			}

			var span = Assert.Single(_activities, a => a.OperationName == $"publish {Exchange}");
			Assert.Equal(parentSpanId, span.ParentSpanId.ToString());
		}

		[Fact]
		public async Task PublishAsync_RecordsErrorAndRethrowsWhenBrokerFails()
		{
			var (publisher, channel) = CreatePublisher();
			channel.Setup(c => c.BasicPublishAsync(
					It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
					It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
				.Returns(ValueTask.FromException(new InvalidOperationException("broker down")));

			await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(new TelemetryEvent()).AsTask());

			var span = Assert.Single(_activities, a => a.OperationName == $"publish {Exchange}");
			Assert.Equal(ActivityStatusCode.Error, span.Status);
			Assert.Equal(typeof(InvalidOperationException).FullName, span.GetTagItem("error.type"));
			Assert.Contains(span.Events, e => e.Name == "exception");
			Assert.DoesNotContain(_measurements, m => m.Instrument == "douglasmq.messages.published");
			Assert.Contains(_measurements, m => m.Instrument == "douglasmq.publish.duration"
				&& (string?)m.Tags["error.type"] == typeof(InvalidOperationException).FullName);
		}

		[Fact]
		public async Task PublishAsync_RecordsPublishMetrics()
		{
			var (publisher, _) = CreatePublisher();

			await publisher.PublishAsync(new TelemetryEvent());

			var published = Assert.Single(_measurements, m => m.Instrument == "douglasmq.messages.published");
			Assert.Equal(1, published.Value);
			Assert.Equal(Exchange, published.Tags["messaging.destination.name"]);
			Assert.Equal(nameof(TelemetryEvent), published.Tags["douglasmq.message.type"]);
			Assert.Single(_measurements, m => m.Instrument == "douglasmq.publish.duration");
		}

		// --- Consuming ---

		[Fact]
		public async Task ProcessDelivery_ContinuesTraceFromMessageHeaders()
		{
			var (consumer, channel, _) = CreateConsumer(new TelemetryHandler());
			var remoteParent = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
			var headers = new Dictionary<string, object?>
			{
				// RabbitMQ.Client delivers string headers as UTF-8 bytes.
				["traceparent"] = Encoding.UTF8.GetBytes($"00-{remoteParent.TraceId}-{remoteParent.SpanId}-01"),
			};

			await consumer.HandleDeliveryAsync(channel.Object, Delivery(headers));

			var span = Assert.Single(_activities, a => a.OperationName == $"process {Queue}");
			Assert.Equal(ActivityKind.Consumer, span.Kind);
			Assert.Equal(remoteParent.TraceId, span.TraceId);
			Assert.Equal(remoteParent.SpanId, span.ParentSpanId);
			Assert.Equal(nameof(TelemetryHandler), span.GetTagItem("douglasmq.handler"));
			Assert.Equal(Queue, span.GetTagItem("messaging.destination.subscription.name"));
			Assert.Equal("ack", span.GetTagItem("douglasmq.outcome"));
			Assert.Equal(0, span.GetTagItem("douglasmq.retry_count"));
		}

		[Fact]
		public async Task ProcessDelivery_RecordsAckOutcomeMetrics()
		{
			var (consumer, channel, _) = CreateConsumer(new TelemetryHandler());

			await consumer.HandleDeliveryAsync(channel.Object, Delivery());

			var consumed = Assert.Single(_measurements, m => m.Instrument == "douglasmq.messages.consumed");
			Assert.Equal("ack", consumed.Tags["douglasmq.outcome"]);
			Assert.Equal(nameof(TelemetryHandler), consumed.Tags["douglasmq.handler"]);
			Assert.Single(_measurements, m => m.Instrument == "douglasmq.process.duration");
			Assert.DoesNotContain(_measurements, m => m.Instrument == "douglasmq.messages.dead_lettered");
		}

		[Fact]
		public async Task ProcessDelivery_RecordsRetriesThenSuccess()
		{
			var handler = new TelemetryHandler { FailuresBeforeSuccess = 2 };
			var (consumer, channel, _) = CreateConsumer(handler, retryCount: 3);

			await consumer.HandleDeliveryAsync(channel.Object, Delivery());

			var span = Assert.Single(_activities, a => a.OperationName == $"process {Queue}");
			Assert.Equal(2, span.Events.Count(e => e.Name == "douglasmq.retry"));
			Assert.Equal(2, span.Events.Count(e => e.Name == "exception"));
			Assert.Equal("ack", span.GetTagItem("douglasmq.outcome"));
			Assert.Equal(2, span.GetTagItem("douglasmq.retry_count"));
			Assert.Equal(ActivityStatusCode.Unset, span.Status);
			Assert.Equal(2, _measurements.Where(m => m.Instrument == "douglasmq.handler.retries").Sum(m => m.Value));
		}

		[Fact]
		public async Task ProcessDelivery_NestsOneAttemptSpanPerHandlerAttempt()
		{
			var handler = new TelemetryHandler { FailuresBeforeSuccess = 1 };
			var (consumer, channel, _) = CreateConsumer(handler, retryCount: 2);

			await consumer.HandleDeliveryAsync(channel.Object, Delivery());

			var delivery = Assert.Single(_activities, a => a.OperationName == $"process {Queue}");
			var attempts = _activities.Where(a => a.OperationName == $"{Queue} process").ToList();
			Assert.Equal(2, attempts.Count);
			Assert.All(attempts, a =>
			{
				Assert.Equal(delivery.SpanId, a.ParentSpanId);
				Assert.Equal(delivery.TraceId, a.TraceId);
				Assert.Equal(ActivityKind.Consumer, a.Kind);
				Assert.Equal(Queue, a.GetTagItem("messaging.consumer.group.name"));
			});
			Assert.Equal(ActivityStatusCode.Error, attempts[0].Status);
			Assert.Equal(ActivityStatusCode.Unset, attempts[1].Status);
		}

		[Fact]
		public async Task ProcessDelivery_MarksSpanAsErrorWhenDeadLettered()
		{
			var handler = new TelemetryHandler { FailuresBeforeSuccess = int.MaxValue };
			var (consumer, channel, _) = CreateConsumer(handler, retryCount: 1);

			await consumer.HandleDeliveryAsync(channel.Object, Delivery());

			var span = Assert.Single(_activities, a => a.OperationName == $"process {Queue}");
			Assert.Equal(ActivityStatusCode.Error, span.Status);
			Assert.Equal("dead_lettered", span.GetTagItem("douglasmq.outcome"));
			Assert.Equal(typeof(InvalidOperationException).FullName, span.GetTagItem("error.type"));

			var deadLettered = Assert.Single(_measurements, m => m.Instrument == "douglasmq.messages.dead_lettered");
			Assert.Equal(typeof(InvalidOperationException).FullName, deadLettered.Tags["error.type"]);
			var consumed = Assert.Single(_measurements, m => m.Instrument == "douglasmq.messages.consumed");
			Assert.Equal("dead_lettered", consumed.Tags["douglasmq.outcome"]);

			channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), false, false, It.IsAny<CancellationToken>()), Times.Once);
		}

		[Fact]
		public async Task ProcessDelivery_DeadLettersWhenDeserializationThrows()
		{
			var (consumer, channel, _) = CreateConsumer(new TelemetryHandler(), serializer: new ThrowingSerializer());

			await consumer.HandleDeliveryAsync(channel.Object, Delivery());

			var span = Assert.Single(_activities, a => a.OperationName == $"process {Queue}");
			Assert.Equal(ActivityStatusCode.Error, span.Status);
			Assert.Equal(typeof(FormatException).FullName, span.GetTagItem("error.type"));
			channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), false, false, It.IsAny<CancellationToken>()), Times.Once);
		}

		// --- Helpers ---

		private (Publisher Publisher, Mock<IChannel> Channel) CreatePublisher()
		{
			var channel = new Mock<IChannel>();
			var connection = new Mock<IConnection>();
			connection
				.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(channel.Object);

			var publisher = new Publisher(connection.Object, new JsonSerializer(), NullLogger<Publisher>.Instance, new Configuration());
			return (publisher, channel);
		}

		private static (Consumer Consumer, Mock<IChannel> Channel, Mock<IConnection> Connection) CreateConsumer(
			TelemetryHandler handler,
			int retryCount = 0,
			ISerializer? serializer = null)
		{
			var channel = new Mock<IChannel>();
			var connection = new Mock<IConnection>();
			channel.SetupGet(c => c.IsOpen).Returns(true);
			var configuration = new Configuration { RetryCount = retryCount, RetryDelayMs = 1 };

			var consumer = Consumer.Create(
				connection.Object,
				HandlerRegistration.For(HandlerInvoker.Create(handler)),
				serializer ?? new JsonSerializer(),
				configuration,
				NullLogger<Consumer>.Instance);

			return (consumer, channel, connection);
		}

		private static BasicDeliverEventArgs Delivery(IDictionary<string, object?>? headers = null)
		{
			var body = Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(new TelemetryEvent()));
			var properties = new BasicProperties { Headers = headers };
			return new BasicDeliverEventArgs("tag", 1, false, Exchange, RoutingKey, properties, body);
		}

		private void Record<T>(Instrument instrument, T value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
			where T : struct
		{
			var dictionary = new Dictionary<string, object?>();
			foreach (var tag in tags)
			{
				dictionary[tag.Key] = tag.Value;
			}

			_measurements.Enqueue((instrument.Name, Convert.ToDouble(value), dictionary));
		}

		// --- Test doubles ---

		[Message(Exchange, RoutingKey)]
		public sealed class TelemetryEvent : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();
			public Guid TraceId { get; set; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, Exchange, Queue, routingKey: RoutingKey)]
		public sealed class TelemetryHandler : IHandler<TelemetryEvent>
		{
			private int _calls;

			public int FailuresBeforeSuccess { get; set; }

			public Task HandleAsync(TelemetryEvent message)
			{
				if (Interlocked.Increment(ref _calls) <= FailuresBeforeSuccess)
				{
					throw new InvalidOperationException("handler failed");
				}

				return Task.CompletedTask;
			}
		}

		private sealed class JsonSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => System.Text.Json.JsonSerializer.Deserialize(data, type);

			public string Serialize<T>(T data) => System.Text.Json.JsonSerializer.Serialize(data);
		}

		private sealed class ThrowingSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => throw new FormatException("bad payload");

			public string Serialize<T>(T data) => "{}";
		}
	}
}
