using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	[Collection(RabbitMqCollection.CollectionName)]
	[Trait("Category", "Integration")]
	public sealed class RetryIntegrationTests
	{
		private readonly RabbitMqFixture _fixture;

		public RetryIntegrationTests(RabbitMqFixture fixture)
		{
			_fixture = fixture;
		}

		[Fact]
		public async Task Handler_SucceedsOnSecondAttempt_MessageIsAcked()
		{
			var attempts = 0;
			var success = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			var handler = new FlakyHandler(() =>
			{
				attempts++;
				if (attempts < 2)
				{
					throw new InvalidOperationException("Simulated transient failure.");
				}

				success.TrySetResult(true);
			});

			var builder = _fixture.CreateBuilder()
				.Configure(c => { c.RetryCount = 3; c.RetryDelayMs = 10; })
				.AddHandler(handler);

			await using var consumers = await builder.StartConsumersAsync();
			await using var publisher = (Publisher)await builder.BuildPublisherAsync();

			await publisher.PublishAsync(new FlakyEvent { Tag = "retry-test" });

			var acked = await success.Task.WaitAsync(TimeSpan.FromSeconds(15));
			Assert.True(acked);
			Assert.Equal(2, attempts);
		}

		// --- Test doubles ---

		[Message("retry.test.exchange", "flaky.key")]
		public class FlakyEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public string Tag { get; set; } = string.Empty;
		}

		[Handler(ExchangeType.direct, "retry.test.exchange", "retry.test.queue", "flaky.key")]
		public class FlakyHandler : IHandler<FlakyEvent>
		{
			private readonly Action _action;
			public FlakyHandler(Action action) => _action = action;
			public Task HandleAsync(FlakyEvent message) { _action(); return Task.CompletedTask; }
		}
	}
}
