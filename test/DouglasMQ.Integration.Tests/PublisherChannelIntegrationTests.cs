using System;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class PublisherChannelIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public PublisherChannelIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task PublishedMessage_IsPersistent_AndCarriesIdentityProperties()
		{
			var builder = _fixture.CreateBuilder();
			await using var connection = await builder.CreateConnectionAsync();
			await using var channel = await connection.CreateChannelAsync();
			await channel.ExchangeDeclareAsync("props.exchange", "direct", durable: true, autoDelete: false);
			await channel.QueueDeclareAsync("props.queue", durable: true, exclusive: false, autoDelete: false);
			await channel.QueuePurgeAsync("props.queue");
			await channel.QueueBindAsync("props.queue", "props.exchange", "props.key");

			var sent = new PropsEvent();
			await using (var publisher = (Publisher)await builder.Configure(c => c.PublisherConfirms = true).BuildPublisherAsync())
			{
				await publisher.PublishAsync(sent);
			}

			var result = await channel.BasicGetAsync("props.queue", autoAck: true);

			Assert.NotNull(result);
			Assert.True(result.BasicProperties.Persistent, "Message should be published with delivery mode 2 (persistent).");
			Assert.Equal(sent.MessageId.ToString(), result.BasicProperties.MessageId);
			Assert.Equal(sent.TraceId.ToString(), result.BasicProperties.CorrelationId);
			Assert.Equal(typeof(PropsEvent).FullName, result.BasicProperties.Type);
		}

		[Fact]
		public async Task PublishToMissingExchange_DoesNotBreakSubsequentPublishes()
		{
			var builder = _fixture.CreateBuilder().Configure(c => c.PublisherConfirms = true);
			await using var connection = await builder.CreateConnectionAsync();
			await using var channel = await connection.CreateChannelAsync();
			await channel.ExchangeDeclareAsync("props.exchange", "direct", durable: true, autoDelete: false);
			await channel.QueueDeclareAsync("recovery.queue", durable: true, exclusive: false, autoDelete: false);
			await channel.QueuePurgeAsync("recovery.queue");
			await channel.QueueBindAsync("recovery.queue", "props.exchange", "recovery.key");

			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			// The broker answers with 404 NOT_FOUND and closes the publisher's channel.
			await Assert.ThrowsAnyAsync<Exception>(() => publisher.PublishAsync(new MissingExchangeEvent()).AsTask());

			// Before the fix this threw AlreadyClosedException forever after.
			await publisher.PublishAsync(new RecoveryEvent());

			var result = await channel.BasicGetAsync("recovery.queue", autoAck: true);
			Assert.NotNull(result);
		}

		// --- Test messages ---

		[Message("props.exchange", "props.key")]
		public sealed class PropsEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message("props.exchange", "recovery.key")]
		public sealed class RecoveryEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message("exchange.that.does.not.exist", "any.key")]
		public sealed class MissingExchangeEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}
	}
}
