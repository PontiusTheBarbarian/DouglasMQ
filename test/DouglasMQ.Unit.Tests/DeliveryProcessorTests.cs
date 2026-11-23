using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// Verifies the processing rules — deserialization, retries, dead-lettering, shutdown — with no
	/// RabbitMQ types involved: a body goes in, a <see cref="DeliveryOutcome"/> comes out.
	/// </summary>
	public class DeliveryProcessorTests
	{
		private static readonly HandlerAttribute Binding = new(ExchangeType.direct, "test.exchange", "test.queue", "test.key");

		private static DeliveryProcessor CreateProcessor(object handler, int retryCount = 2, int retryDelayMs = 0, Type? messageType = null)
			=> new(
				new JsonSerializer(),
				HandlerInvoker.Create(handler, messageType),
				new RetryPolicy(retryCount, retryDelayMs),
				NullLogger.Instance,
				Binding);

		private static Task<DeliveryOutcome> ProcessAsync(DeliveryProcessor processor, string body, CancellationToken stoppingToken = default)
			=> processor.ProcessAsync(Encoding.UTF8.GetBytes(body), DeliveryInfo.Empty, stoppingToken);

		private static string ValidBody => """{"Value":"hello"}""";

		[Fact]
		public async Task SuccessfulHandler_Acks()
		{
			var handler = new RecordingHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler), ValidBody);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			Assert.Equal("hello", handler.Received?.Value);
		}

		[Fact]
		public async Task HandlerFailsOnce_IsRetriedThenAcks()
		{
			var failures = 1;
			var handler = new RecordingHandler { Throw = () => failures-- > 0 ? new InvalidOperationException("transient") : null };

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 2), ValidBody);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			Assert.Equal(2, handler.Attempts);
		}

		[Fact]
		public async Task HandlerAlwaysThrows_RetriesThenDeadLetters()
		{
			var handler = new RecordingHandler { Throw = () => new InvalidOperationException("boom") };

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 2), ValidBody);

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(3, handler.Attempts);
		}

		[Fact]
		public async Task RetryCountZero_DeadLettersAfterOneAttempt()
		{
			var handler = new RecordingHandler { Throw = () => new InvalidOperationException("boom") };

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 0), ValidBody);

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task HandlerThrowsOperationCanceled_IsTreatedAsFailure_NotAbandoned()
		{
			// e.g. an HttpClient timeout inside the handler. Previously this was rethrown and the
			// delivery was never settled.
			var handler = new RecordingHandler { Throw = () => new TaskCanceledException("timeout") };

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 1), ValidBody);

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(2, handler.Attempts);
		}

		[Fact]
		public async Task NonRetryableException_DeadLettersAfterOneAttempt_WithoutWaiting()
		{
			var handler = new RecordingHandler { Throw = () => new NonRetryableException("Quantity must be positive.") };
			var processor = CreateProcessor(handler, retryCount: 3, retryDelayMs: 60_000);

			var outcome = await ProcessAsync(processor, ValidBody).WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task NonRetryableException_AsInnerException_AlsoSkipsRetries()
		{
			var handler = new RecordingHandler
			{
				Throw = () => new InvalidOperationException("wrapped", new NonRetryableException("invalid")),
			};

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 3, retryDelayMs: 60_000), ValidBody).WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task NonRetryableException_InsideAggregateException_AlsoSkipsRetries()
		{
			var handler = new RecordingHandler
			{
				Throw = () => new AggregateException(new InvalidOperationException("other"), new NonRetryableException("invalid")),
			};

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 3, retryDelayMs: 60_000), ValidBody).WaitAsync(TimeSpan.FromSeconds(5));

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task NonRetryableException_IsLoggedAsWarning_NamingHandlerMessageTypeAndReason()
		{
			var logger = new RecordingLogger();
			var processor = new DeliveryProcessor(
				new JsonSerializer(),
				HandlerInvoker.Create(new RecordingHandler { Throw = () => new NonRetryableException("Quantity must be positive.") }),
				new RetryPolicy(3, 0),
				logger,
				Binding);

			await processor.ProcessAsync(Encoding.UTF8.GetBytes(ValidBody), DeliveryInfo.Empty, CancellationToken.None);

			var entry = Assert.Single(logger.Entries, e => e.Exception is NonRetryableException);
			Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
			Assert.Contains(nameof(RecordingHandler), entry.Message);
			Assert.Contains(nameof(TestMessage), entry.Message);
			Assert.Contains("Quantity must be positive.", entry.Message);
			Assert.DoesNotContain(logger.Entries, e => e.Level >= Microsoft.Extensions.Logging.LogLevel.Error);
		}

		[Fact]
		public async Task MalformedBody_DeadLettersWithoutCallingHandler()
		{
			var handler = new RecordingHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler), "{ not json");

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(0, handler.Attempts);
		}

		[Fact]
		public async Task NullDeserializationResult_DeadLetters()
		{
			var handler = new RecordingHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler), "null");

			Assert.Equal(DeliveryOutcome.DeadLetter, outcome);
			Assert.Equal(0, handler.Attempts);
		}

		[Fact]
		public async Task MessageTypeWithoutMessageAttribute_IsStillConsumed()
		{
			var handler = new UnattributedHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler), ValidBody);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			Assert.True(handler.Called);
		}

		[Fact]
		public async Task ExplicitInterfaceImplementation_IsInvoked()
		{
			var handler = new ExplicitHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler), ValidBody);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			Assert.True(handler.Called);
		}

		[Fact]
		public void HandlerForMultipleMessageTypes_WithoutMessageType_FailsFastAtCreation()
		{
			Assert.Throws<InvalidOperationException>(() => CreateProcessor(new MultiHandler()));
		}

		[Fact]
		public async Task HandlerForMultipleMessageTypes_WithMessageType_DispatchesToThatInterface()
		{
			var handler = new MultiHandler();

			var outcome = await ProcessAsync(CreateProcessor(handler, messageType: typeof(UnattributedMessage)), ValidBody);

			Assert.Equal(DeliveryOutcome.Ack, outcome);
			Assert.Equal(nameof(UnattributedMessage), handler.CalledWith);
		}

		[Fact]
		public async Task ShutdownDuringRetryDelay_Abandons()
		{
			var handler = new RecordingHandler { Throw = () => new InvalidOperationException("boom") };
			using var stopping = new CancellationTokenSource();

			var processing = ProcessAsync(CreateProcessor(handler, retryCount: 3, retryDelayMs: 60_000), ValidBody, stopping.Token);
			await stopping.CancelAsync();

			Assert.Equal(DeliveryOutcome.Abandon, await processing.WaitAsync(TimeSpan.FromSeconds(5)));
			Assert.Equal(1, handler.Attempts);
		}

		[Fact]
		public async Task HandlerCancelledByShutdown_Abandons()
		{
			using var stopping = new CancellationTokenSource();
			var handler = new RecordingHandler
			{
				Throw = () =>
				{
					stopping.Cancel();
					return new OperationCanceledException(stopping.Token);
				},
			};

			var outcome = await ProcessAsync(CreateProcessor(handler, retryCount: 3), ValidBody, stopping.Token);

			Assert.Equal(DeliveryOutcome.Abandon, outcome);
			Assert.Equal(1, handler.Attempts);
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
			public Func<Exception?>? Throw { get; init; }

			public int Attempts { get; private set; }

			public TestMessage? Received { get; private set; }

			public Task HandleAsync(TestMessage message)
			{
				Attempts++;
				if (Throw?.Invoke() is { } exception)
				{
					throw exception;
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
