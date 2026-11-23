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
	/// except while shutting down, when it is left for the broker to redeliver. Processing rules
	/// themselves are covered by <see cref="DeliveryProcessorTests"/>.
	/// </summary>
	public class ConsumerTests
	{
		private const ulong DeliveryTag = 42;

		private readonly Mock<IChannel> _channel = new();

		public ConsumerTests()
		{
			_channel.SetupGet(c => c.IsOpen).Returns(true);
		}

		private static Consumer CreateConsumer(object handler, int retryCount = 2, int retryDelayMs = 0)
			=> Consumer.Create(
				new Mock<IConnection>().Object,
				HandlerRegistration.For(HandlerInvoker.Create(handler)),
				new SystemTextJsonSerializer(),
				new Configuration { RetryCount = retryCount, RetryDelayMs = retryDelayMs },
				NullLogger.Instance);

		private static BasicDeliverEventArgs Delivery(string body) => new(
			consumerTag: "tag",
			deliveryTag: DeliveryTag,
			redelivered: false,
			exchange: "test.exchange",
			routingKey: "test.key",
			properties: new BasicProperties(),
			body: Encoding.UTF8.GetBytes(body));

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
		public async Task Handled_Acks()
		{
			await CreateConsumer(new Handler()).HandleDeliveryAsync(_channel.Object, Delivery("{}"));

			VerifyAcked();
		}

		[Fact]
		public async Task Failed_NacksWithoutRequeue()
		{
			var consumer = CreateConsumer(new Handler { Fail = true }, retryCount: 0);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery("{}"));

			VerifyDeadLettered();
		}

		[Fact]
		public async Task ShutdownDuringRetryDelay_LeavesDeliveryForRedelivery()
		{
			var consumer = CreateConsumer(new Handler { Fail = true }, retryCount: 3, retryDelayMs: 60_000);

			var handling = consumer.HandleDeliveryAsync(_channel.Object, Delivery("{}"));
			await consumer.DisposeAsync();
			await handling.WaitAsync(TimeSpan.FromSeconds(5));

			VerifyNotSettled();
		}

		[Fact]
		public async Task SettleFailsBecauseChannelClosed_DoesNotThrow()
		{
			_channel.SetupGet(c => c.IsOpen).Returns(false);
			_channel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.ThrowsAsync(new InvalidOperationException("channel closed"));

			await CreateConsumer(new Handler()).HandleDeliveryAsync(_channel.Object, Delivery("{}"));
		}

		[Fact]
		public void Create_WithUndecoratedHandler_FailsWithClearError()
		{
			var ex = Assert.Throws<InvalidOperationException>(() => HandlerRegistration.For(HandlerInvoker.Create(new UndecoratedHandler())));
			Assert.Contains(nameof(UndecoratedHandler), ex.Message);
		}

		// --- Test doubles ---

		public sealed class Message : IEvent
		{
			public Guid MessageId { get; set; } = Guid.NewGuid();

			public Guid TraceId { get; set; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "test.exchange", "test.queue", "test.key")]
		private sealed class Handler : IHandler<Message>
		{
			public bool Fail { get; init; }

			public Task HandleAsync(Message message) => Fail ? throw new InvalidOperationException("boom") : Task.CompletedTask;
		}

		private sealed class UndecoratedHandler : IHandler<Message>
		{
			public Task HandleAsync(Message message) => Task.CompletedTask;
		}
	}
}
