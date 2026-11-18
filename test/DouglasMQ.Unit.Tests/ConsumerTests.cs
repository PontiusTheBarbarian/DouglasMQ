using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// Verifies the consumer's settlement contract: every delivery is acked or nacked exactly once,
	/// except while shutting down, when it is left for the broker to redeliver.
	/// </summary>
	public class ConsumerTests
	{
		private const ulong DeliveryTag = 42;

		private static readonly HandlerAttribute Attribute = new(ExchangeType.direct, "test.exchange", "test.queue", "test.key");

		private readonly Mock<IChannel> _channel = new();

		public ConsumerTests()
		{
			_channel.SetupGet(c => c.IsOpen).Returns(true);
		}

		private static Consumer CreateConsumer(object handler, ISerializer? serializer = null, int retryCount = 2, int retryDelayMs = 0, Type? messageType = null)
			=> Consumer.Create(
				new Mock<IConnection>().Object,
				serializer ?? new JsonSerializer(),
				NullLogger.Instance,
				new Configuration { RetryCount = retryCount, RetryDelayMs = retryDelayMs },
				Attribute,
				handler,
				messageType);

		private static BasicDeliverEventArgs Delivery(string body) => new(
			consumerTag: "tag",
			deliveryTag: DeliveryTag,
			redelivered: false,
			exchange: "test.exchange",
			routingKey: "test.key",
			properties: new BasicProperties(),
			body: Encoding.UTF8.GetBytes(body));

		private static string ValidBody => """{"Value":"hello"}""";

		private void VerifyAcked()
		{
			_channel.Verify(c => c.BasicAckAsync(DeliveryTag, false, It.IsAny<CancellationToken>()), Times.Once);
			_channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
		}

		private void VerifyDeadLettered()
		{
			_channel.Verify(c => c.BasicNackAsync(DeliveryTag, false, false, It.IsAny<CancellationToken>()), Times.Once);
			_channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
		}

		private void VerifyNotSettled()
		{
			_channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
			_channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
		}

		[Fact]
		public async Task SuccessfulHandler_Acks()
		{
			var handler = new RecordingHandler();
			var consumer = CreateConsumer(handler);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.Equal("hello", handler.Received?.Value);
			VerifyAcked();
		}

		[Fact]
		public async Task HandlerAlwaysThrows_RetriesThenDeadLetters()
		{
			var handler = new RecordingHandler { Throw = () => new InvalidOperationException("boom") };
			var consumer = CreateConsumer(handler, retryCount: 2);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.Equal(3, handler.Attempts);
			VerifyDeadLettered();
		}

		[Fact]
		public async Task HandlerThrowsOperationCanceled_IsTreatedAsFailure_NotLeftUnacked()
		{
			// e.g. an HttpClient timeout inside the handler. Previously this was rethrown and the
			// delivery was never settled.
			var handler = new RecordingHandler { Throw = () => new TaskCanceledException("timeout") };
			var consumer = CreateConsumer(handler, retryCount: 1);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.Equal(2, handler.Attempts);
			VerifyDeadLettered();
		}

		[Fact]
		public async Task MalformedBody_DeadLettersWithoutCallingHandler()
		{
			var handler = new RecordingHandler();
			var consumer = CreateConsumer(handler);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery("{ not json"));

			Assert.Equal(0, handler.Attempts);
			VerifyDeadLettered();
		}

		[Fact]
		public async Task NullDeserializationResult_DeadLetters()
		{
			var handler = new RecordingHandler();
			var consumer = CreateConsumer(handler);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery("null"));

			Assert.Equal(0, handler.Attempts);
			VerifyDeadLettered();
		}

		[Fact]
		public async Task MessageTypeWithoutMessageAttribute_IsStillConsumed()
		{
			var handler = new UnattributedHandler();
			var consumer = CreateConsumer(handler);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.True(handler.Called);
			VerifyAcked();
		}

		[Fact]
		public async Task ExplicitInterfaceImplementation_IsInvoked()
		{
			var handler = new ExplicitHandler();
			var consumer = CreateConsumer(handler);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.True(handler.Called);
			VerifyAcked();
		}

		[Fact]
		public void HandlerForMultipleMessageTypes_WithoutMessageType_FailsFastAtCreation()
		{
			Assert.Throws<InvalidOperationException>(() => CreateConsumer(new MultiHandler()));
		}

		[Fact]
		public async Task HandlerForMultipleMessageTypes_WithMessageType_DispatchesToThatInterface()
		{
			var handler = new MultiHandler();
			var consumer = CreateConsumer(handler, messageType: typeof(UnattributedMessage));

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));

			Assert.Equal(nameof(UnattributedMessage), handler.CalledWith);
			VerifyAcked();
		}

		[Fact]
		public async Task ShutdownDuringRetryDelay_LeavesDeliveryForRedelivery()
		{
			var handler = new RecordingHandler { Throw = () => new InvalidOperationException("boom") };
			var consumer = CreateConsumer(handler, retryCount: 3, retryDelayMs: 60_000);

			var handling = consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));
			await consumer.DisposeAsync();
			await handling.WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(1, handler.Attempts);
			VerifyNotSettled();
		}

		[Fact]
		public async Task SettleFailsBecauseChannelClosed_DoesNotThrow()
		{
			_channel.SetupGet(c => c.IsOpen).Returns(false);
			_channel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.ThrowsAsync(new InvalidOperationException("channel closed"));
			var consumer = CreateConsumer(new RecordingHandler());

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery(ValidBody));
		}

		[Theory]
		[InlineData(1000, 1, 1000)]
		[InlineData(1000, 2, 2000)]
		[InlineData(1000, 4, 8000)]
		[InlineData(int.MaxValue, 40, 300_000)] // capped at 5 minutes instead of overflowing
		public void GetRetryDelay_IsExponentialAndCapped(int baseDelayMs, int attempt, double expectedMs)
		{
			var consumer = CreateConsumer(new RecordingHandler(), retryDelayMs: baseDelayMs);

			Assert.Equal(expectedMs, consumer.GetRetryDelay(attempt).TotalMilliseconds);
		}

		// --- Test doubles ---

		private sealed class JsonSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => System.Text.Json.JsonSerializer.Deserialize(data, type);

			public string Serialize<T>(T data) => System.Text.Json.JsonSerializer.Serialize(data);
		}

		[Message("test.exchange", "test.key")]
		public sealed class TestMessage : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();

			public Guid TraceId { get; set; } = Guid.NewGuid();

			public string Value { get; set; } = string.Empty;
		}

		/// <summary>No [Message] attribute: consumers should not require one.</summary>
		public sealed class UnattributedMessage : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();

			public Guid TraceId { get; set; } = Guid.NewGuid();

			public string Value { get; set; } = string.Empty;
		}

		private sealed class RecordingHandler : IHandler<TestMessage>
		{
			public Func<Exception>? Throw { get; init; }

			public int Attempts { get; private set; }

			public TestMessage? Received { get; private set; }

			public Task HandleAsync(TestMessage message)
			{
				Attempts++;
				if (Throw is not null)
				{
					throw Throw();
				}

				Received = message;
				return Task.CompletedTask;
			}
		}

		private sealed class UnattributedHandler : IHandler<UnattributedMessage>
		{
			public bool Called { get; private set; }

			public Task HandleAsync(UnattributedMessage message)
			{
				Called = true;
				return Task.CompletedTask;
			}
		}

		private sealed class ExplicitHandler : IHandler<TestMessage>
		{
			public bool Called { get; private set; }

			Task IHandler<TestMessage>.HandleAsync(TestMessage message)
			{
				Called = true;
				return Task.CompletedTask;
			}
		}

		private sealed class MultiHandler : IHandler<TestMessage>, IHandler<UnattributedMessage>
		{
			public string? CalledWith { get; private set; }

			public Task HandleAsync(TestMessage message)
			{
				CalledWith = nameof(TestMessage);
				return Task.CompletedTask;
			}

			public Task HandleAsync(UnattributedMessage message)
			{
				CalledWith = nameof(UnattributedMessage);
				return Task.CompletedTask;
			}
		}
	}
}
