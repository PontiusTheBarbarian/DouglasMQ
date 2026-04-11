using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class DlxIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public DlxIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task Handler_ThatAlwaysThrows_MessageEndsUpInDlxQueue()
		{
			var dlxReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			var handler = new AlwaysThrowingHandler();

			var builder = _fixture.CreateBuilder().AddHandler(handler);
			var connection = await builder.CreateConnectionAsync();

			// Listen on the DLX queue before starting consumers
			var dlxChannel = await connection.CreateChannelAsync();
			await dlxChannel.ExchangeDeclareAsync("dlx.exchange", "direct", durable: true, autoDelete: false);
			await dlxChannel.QueueDeclareAsync("dlx.queue", durable: true, exclusive: false, autoDelete: false);
			await dlxChannel.QueueBindAsync("dlx.queue", "dlx.exchange", "dlx_routing_key");

			var dlxConsumer = new AsyncEventingBasicConsumer(dlxChannel);
			dlxConsumer.ReceivedAsync += (_, _) => { dlxReceived.TrySetResult(true); return Task.CompletedTask; };
			await dlxChannel.BasicConsumeAsync("dlx.queue", autoAck: true, consumer: dlxConsumer);

			// Start consumers and publish
			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			await publisher.PublishAsync(new AlwaysFailEvent { Value = 42 });

			var landed = await dlxReceived.Task.WaitAsync(TimeSpan.FromSeconds(15));
			Assert.True(landed, "Message did not arrive in DLX queue within timeout.");

			await dlxChannel.CloseAsync();
			await dlxChannel.DisposeAsync();
			await connection.CloseAsync();
			await connection.DisposeAsync();
		}

		// --- Test doubles ---

		[Message("dlx.test.exchange", "dlx.key")]
		public class AlwaysFailEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public int Value { get; set; }
		}

		[Handler(ExchangeType.direct, "dlx.test.exchange", "dlx.test.queue", "dlx.key")]
		public class AlwaysThrowingHandler : IHandler<AlwaysFailEvent>
		{
			public Task HandleAsync(AlwaysFailEvent message) =>
				throw new InvalidOperationException("Simulated handler failure.");
		}
	}
}
