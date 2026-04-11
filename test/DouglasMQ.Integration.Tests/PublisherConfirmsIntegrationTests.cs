using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class PublisherConfirmsIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public PublisherConfirmsIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task PublishAsync_WithConfirmsEnabled_CompletesWithoutException()
		{
			var received = new TaskCompletionSource<ConfirmableEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
			var handler = new ConfirmableEventHandler(e => received.TrySetResult(e));

			var builder = _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = true)
				.AddHandler(handler);

			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			var sent = new ConfirmableEvent { Value = 99 };

			// Should not throw — broker confirms message before returning
			await publisher.PublishAsync(sent);

			var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
			Assert.Equal(99, result.Value);
		}

		// --- Test doubles ---

		[Message("confirms.exchange", "confirms.key")]
		public class ConfirmableEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public int Value { get; set; }
		}

		[Handler(ExchangeType.direct, "confirms.exchange", "confirms.queue", "confirms.key")]
		public class ConfirmableEventHandler : IHandler<ConfirmableEvent>
		{
			private readonly Action<ConfirmableEvent> _onReceived;
			public ConfirmableEventHandler(Action<ConfirmableEvent> onReceived) => _onReceived = onReceived;
			public Task HandleAsync(ConfirmableEvent message) { _onReceived(message); return Task.CompletedTask; }
		}
	}
}
