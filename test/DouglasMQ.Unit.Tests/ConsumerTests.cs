using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
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

		/// <summary>A consumer started on a mocked connection whose only channel is <see cref="_channel"/>, recording call order.</summary>
		private async Task<(Consumer Consumer, List<string> Calls)> StartedConsumerAsync(object handler, RecordingLogger? logger = null)
		{
			var calls = new List<string>();
			var open = true;
			_channel.SetupGet(c => c.IsOpen).Returns(() => open);
			_channel.Setup(c => c.QueueDeclareAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(new QueueDeclareOk("test.queue", 0, 0));
			_channel.Setup(c => c.BasicConsumeAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(), It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync("consumer-tag");
			_channel.Setup(c => c.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Callback(() => { lock (calls) { calls.Add("cancel"); } })
				.Returns(Task.CompletedTask);
			_channel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Callback(() => { lock (calls) { calls.Add("ack"); } })
				.Returns(ValueTask.CompletedTask);
			_channel.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Callback(() => { lock (calls) { calls.Add("close"); } open = false; })
				.Returns(Task.CompletedTask);

			var connection = new Mock<IConnection>();
			connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>())).ReturnsAsync(_channel.Object);

			var consumer = Consumer.Create(
				connection.Object,
				HandlerRegistration.For(HandlerInvoker.Create(handler)),
				new SystemTextJsonSerializer(),
				new Configuration { RetryCount = 0 },
				(ILogger?)logger ?? NullLogger.Instance);
			await consumer.StartAsync(CancellationToken.None);
			return (consumer, calls);
		}

		[Fact]
		public async Task Stop_LetsInFlightHandlerFinishAndSettle_BeforeClosingTheChannel()
		{
			var handler = new BlockingHandler();
			var (consumer, calls) = await StartedConsumerAsync(handler);

			var handling = consumer.HandleDeliveryAsync(_channel.Object, Delivery("{}"));
			await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

			var stopping = consumer.StopAsync(CancellationToken.None);
			await Task.Delay(100);
			Assert.False(stopping.IsCompleted); // still waiting for the handler

			handler.Release.SetResult();
			await Task.WhenAll(handling, stopping).WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(new[] { "cancel", "ack", "close" }, calls);
		}

		[Fact]
		public async Task DeliveryArrivingAfterStopBegan_IsNeitherHandledNorSettled()
		{
			var handler = new Handler();
			var (consumer, calls) = await StartedConsumerAsync(handler);
			await consumer.StopAsync(CancellationToken.None);

			await consumer.HandleDeliveryAsync(_channel.Object, Delivery("{}"));

			VerifyNotSettled();
			Assert.Equal(new[] { "cancel", "close" }, calls);
		}

		[Fact]
		public async Task Stop_WhenHandlerOutlivesTheToken_ClosesAnywayAndWarns()
		{
			var logger = new RecordingLogger();
			var handler = new BlockingHandler();
			var (consumer, calls) = await StartedConsumerAsync(handler, logger);
			_ = consumer.HandleDeliveryAsync(_channel.Object, Delivery("{}"));
			await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

			using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
			await consumer.StopAsync(timeout.Token).WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(new[] { "cancel", "close" }, calls);
			Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("in-flight", StringComparison.Ordinal));
			handler.Release.SetResult();
		}

		[Fact]
		public async Task OverlappingStopAndDispose_CloseTheChannelOnce()
		{
			var (consumer, _) = await StartedConsumerAsync(new Handler());

			await Task.WhenAll(
				consumer.StopAsync(CancellationToken.None),
				consumer.StopAsync(CancellationToken.None),
				consumer.DisposeAsync().AsTask(),
				consumer.DisposeAsync().AsTask());

			_channel.Verify(c => c.BasicCancelAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
			_channel.Verify(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
			_channel.Verify(c => c.DisposeAsync(), Times.Once);
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

		[Handler(ExchangeType.direct, "test.exchange", "test.queue", "test.key")]
		private sealed class BlockingHandler : IHandler<Message>
		{
			public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

			public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

			public async Task HandleAsync(Message message)
			{
				Started.TrySetResult();
				await Release.Task;
			}
		}

		private sealed class UndecoratedHandler : IHandler<Message>
		{
			public Task HandleAsync(Message message) => Task.CompletedTask;
		}
	}
}
