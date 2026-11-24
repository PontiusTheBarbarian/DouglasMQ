using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// R8: a message the broker cannot route must not vanish. Runs against a real broker with no consumers.
	/// </summary>
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class UnroutableMessageIntegrationTests
	{
		private const string BoundExchange = "unroutable.bound.exchange";
		private const string UnboundExchange = "unroutable.unbound.exchange";
		private const string BoundQueue = "unroutable.bound.queue";

		private readonly RabbitMqFixture _fixture;

		public UnroutableMessageIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Theory]
		[InlineData(true)]
		[InlineData(false)]
		public async Task SendAsync_ToMissingExchange_Throws(bool publisherConfirms)
		{
			await using var publisher = (Publisher)await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = publisherConfirms)
				.BuildPublisherAsync();

			var ex = await Assert.ThrowsAsync<UnroutableMessageException>(() => publisher.SendAsync(new MissingExchangeCommand()).AsTask());

			Assert.Equal("unroutable.missing.exchange", ex.Exchange);
			Assert.Equal("missing.key", ex.RoutingKey);
			Assert.Equal(UnroutableMessageException.NotFound, ex.ReplyCode);
		}

		[Fact]
		public async Task SendAsync_WithNoQueueBound_AndConfirmsOn_Throws()
		{
			await DeclareTopologyAsync();
			await using var publisher = (Publisher)await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = true)
				.BuildPublisherAsync();

			var ex = await Assert.ThrowsAsync<UnroutableMessageException>(() => publisher.SendAsync(new UnboundCommand()).AsTask());

			Assert.Equal(UnboundExchange, ex.Exchange);
			Assert.Equal("unbound.key", ex.RoutingKey);
			Assert.Equal(UnroutableMessageException.NoRoute, ex.ReplyCode);

			// The return does not poison the channel: a routable send still succeeds.
			await publisher.SendAsync(new BoundCommand());
		}

		[Fact]
		public async Task PublishAsync_WithNoQueueBound_AndConfirmsOn_Throws()
		{
			await DeclareTopologyAsync();
			await using var publisher = (Publisher)await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = true)
				.BuildPublisherAsync();

			var ex = await Assert.ThrowsAsync<UnroutableMessageException>(() => publisher.PublishAsync(new UnboundEvent()).AsTask());

			Assert.Equal(UnroutableMessageException.NoRoute, ex.ReplyCode);
		}

		[Fact]
		public async Task SendAsync_WithNoQueueBound_AndConfirmsOff_LogsTheReturnAtError()
		{
			await DeclareTopologyAsync();
			var logs = new CollectingLoggerProvider();
			using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(logs).SetMinimumLevel(LogLevel.Trace));
			await using var publisher = (Publisher)await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = false)
				.WithLoggerFactory(loggerFactory)
				.BuildPublisherAsync();

			var sent = new UnboundCommand();
			await publisher.SendAsync(sent);

			var deadline = DateTime.UtcNow.AddSeconds(10);
			while (!logs.Entries.Any(e => e.Level == LogLevel.Error && e.Message.Contains(sent.MessageId.ToString(), StringComparison.Ordinal)))
			{
				Assert.True(DateTime.UtcNow < deadline, "The basic.return was not logged at Error.");
				await Task.Delay(25);
			}

			var entry = logs.Entries.First(e => e.Level == LogLevel.Error);
			Assert.Contains(UnboundExchange, entry.Message, StringComparison.Ordinal);
			Assert.Contains("unbound.key", entry.Message, StringComparison.Ordinal);
		}

		[Theory]
		[InlineData(true)]
		[InlineData(false)]
		public async Task SendAsync_ToBoundExchange_IsDelivered(bool publisherConfirms)
		{
			await DeclareTopologyAsync();
			await using var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			await using var channel = await connection.CreateChannelAsync();
			await channel.QueuePurgeAsync(BoundQueue);

			await using (var publisher = (Publisher)await _fixture.CreateBuilder()
				.Configure(c => c.PublisherConfirms = publisherConfirms)
				.BuildPublisherAsync())
			{
				await publisher.SendAsync(new BoundCommand());
				await publisher.SendAsync(new BoundCommand());
			}

			var deadline = DateTime.UtcNow.AddSeconds(10);
			while ((await channel.MessageCountAsync(BoundQueue)) < 2)
			{
				Assert.True(DateTime.UtcNow < deadline, "The commands were not delivered.");
				await Task.Delay(25);
			}
		}

		[Fact]
		public async Task SendAsync_AfterTheExchangeIsDeclared_Succeeds()
		{
			const string LateExchange = "unroutable.late.exchange";
			await using var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			await using var channel = await connection.CreateChannelAsync();
			await channel.ExchangeDeleteAsync(LateExchange);

			await using var publisher = (Publisher)await _fixture.CreateBuilder().BuildPublisherAsync();

			await Assert.ThrowsAsync<UnroutableMessageException>(() => publisher.SendAsync(new LateCommand()).AsTask());

			// A consumer starts and declares the topology; the publisher must not have cached the failure.
			await channel.ExchangeDeclareAsync(LateExchange, "direct", durable: true, autoDelete: false);
			await channel.QueueDeclareAsync("unroutable.late.queue", durable: true, exclusive: false, autoDelete: false);
			await channel.QueueBindAsync("unroutable.late.queue", LateExchange, "late.key");

			await publisher.SendAsync(new LateCommand());
		}

		private async Task DeclareTopologyAsync()
		{
			await using var connection = await _fixture.CreateBuilder().CreateConnectionAsync();
			await using var channel = await connection.CreateChannelAsync();
			await channel.ExchangeDeclareAsync(BoundExchange, "direct", durable: true, autoDelete: false);
			await channel.QueueDeclareAsync(BoundQueue, durable: true, exclusive: false, autoDelete: false);
			await channel.QueueBindAsync(BoundQueue, BoundExchange, "bound.key");
			await channel.ExchangeDeclareAsync(UnboundExchange, "direct", durable: true, autoDelete: false);
		}

		// --- Test messages ---

		[Message("unroutable.missing.exchange", "missing.key")]
		public sealed class MissingExchangeCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message(UnboundExchange, "unbound.key")]
		public sealed class UnboundCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message(UnboundExchange, "unbound.key")]
		public sealed class UnboundEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message(BoundExchange, "bound.key")]
		public sealed class BoundCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message("unroutable.late.exchange", "late.key")]
		public sealed class LateCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		private sealed class CollectingLoggerProvider : ILoggerProvider, ILogger
		{
			public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

			public ILogger CreateLogger(string categoryName) => this;

			public IDisposable? BeginScope<TState>(TState state)
				where TState : notnull
				=> null;

			public bool IsEnabled(LogLevel logLevel) => true;

			public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
				=> Entries.Enqueue((logLevel, formatter(state, exception)));

			public void Dispose()
			{
			}
		}
	}
}
