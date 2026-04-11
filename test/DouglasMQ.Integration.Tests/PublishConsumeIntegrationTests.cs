using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class PublishConsumeIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public PublishConsumeIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task PublishAsync_DeliveredToHandler()
		{
			var received = new TaskCompletionSource<SampleEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
			var handler = new SampleEventHandler(e => received.TrySetResult(e));

			var builder = _fixture.CreateBuilder().AddHandler(handler);
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			var sent = new SampleEvent { Name = "Integration Test" };
			await publisher.PublishAsync(sent);

			var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
			Assert.Equal("Integration Test", result.Name);
		}

		[Fact]
		public async Task SendAsync_DeliveredToHandler()
		{
			var received = new TaskCompletionSource<SampleCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
			var handler = new SampleCommandHandler(c => received.TrySetResult(c));

			var builder = _fixture.CreateBuilder().AddHandler(handler);
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			var sent = new SampleCommand { Payload = "hello" };
			await publisher.SendAsync(sent);

			var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
			Assert.Equal("hello", result.Payload);
		}

		// --- Test messages and handlers ---

		[Message("integration.exchange", "event.key")]
		public class SampleEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public string Name { get; set; } = string.Empty;
		}

		[Message("integration.exchange", "command.key")]
		public class SampleCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public string Payload { get; set; } = string.Empty;
		}

		[Handler(ExchangeType.direct, "integration.exchange", "integration.event.queue", "event.key")]
		public class SampleEventHandler : IHandler<SampleEvent>
		{
			private readonly Action<SampleEvent> _onReceived;
			public SampleEventHandler(Action<SampleEvent> onReceived) => _onReceived = onReceived;
			public Task HandleAsync(SampleEvent message) { _onReceived(message); return Task.CompletedTask; }
		}

		[Handler(ExchangeType.direct, "integration.exchange", "integration.command.queue", "command.key")]
		public class SampleCommandHandler : IHandler<SampleCommand>
		{
			private readonly Action<SampleCommand> _onReceived;
			public SampleCommandHandler(Action<SampleCommand> onReceived) => _onReceived = onReceived;
			public Task HandleAsync(SampleCommand message) { _onReceived(message); return Task.CompletedTask; }
		}
	}
}
