using System;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// Publishing must keep working after the consumers (and their connection) are shut down.
	/// </summary>
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class ConnectionOwnershipIntegrationTests
	{
		private const string Exchange = "ownership.exchange";
		private readonly RabbitMqFixture _fixture;

		public ConnectionOwnershipIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		private async Task<(IConnection Connection, IChannel Channel)> DeclareQueueAsync(string queue, string routingKey)
		{
			var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			var channel = await connection.CreateChannelAsync();
			await channel.ExchangeDeclareAsync(Exchange, "direct", durable: true, autoDelete: false);
			await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
			await channel.QueuePurgeAsync(queue);
			await channel.QueueBindAsync(queue, Exchange, routingKey);
			return (connection, channel);
		}

		[Fact]
		public async Task BuildAsync_DisposingConsumers_DoesNotBreakPublisher()
		{
			var (connection, channel) = await DeclareQueueAsync("ownership.builder.queue", "builder.key");
			await using var _ = connection;
			await using var __ = channel;

			// Confirms make PublishAsync wait until the broker has routed the message, so the
			// BasicGet below cannot race it.
			var (publisher, consumers) = await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = true)
				.BuildAsync();
			await using var ownedPublisher = (Publisher)publisher;

			await consumers.DisposeAsync();
			await publisher.PublishAsync(new BuilderEvent());

			Assert.NotNull(await channel.BasicGetAsync("ownership.builder.queue", autoAck: true));
		}

		[Fact]
		public async Task DiHost_AfterStop_PublisherStillWorks()
		{
			var (connection, channel) = await DeclareQueueAsync("ownership.host.queue", "host.key");
			await using var _ = connection;
			await using var __ = channel;

			var builder = Host.CreateApplicationBuilder();
			builder.Services.AddSingleton<ISerializer, JsonSerializer>();
			builder.Services.AddDouglasMQ((config, _) =>
			{
				config.HostName = _fixture.HostName;
				config.Port = _fixture.Port;
				config.PublisherConfirms = true;
			});

			using var host = builder.Build();
			await host.StartAsync();
			var publisher = host.Services.GetRequiredService<IPublisher>();

			// Stopping the host closes the consumer connection; the publisher has its own.
			await host.StopAsync();
			await publisher.PublishAsync(new HostEvent());

			Assert.NotNull(await channel.BasicGetAsync("ownership.host.queue", autoAck: true));
		}

		[Message(Exchange, "builder.key")]
		public sealed class BuilderEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message(Exchange, "host.key")]
		public sealed class HostEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}
	}
}
