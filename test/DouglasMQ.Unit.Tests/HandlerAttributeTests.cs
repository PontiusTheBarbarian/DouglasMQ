using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class HandlerAttributeTests
	{
		[Fact]
		public void ConsumerTag_IsFormattedFromExchangeQueueAndRoutingKey()
		{
			var attr = new HandlerAttribute(ExchangeType.direct, "my.exchange", "my.queue", "my.key");
			Assert.Equal("my.exchange.my.queue.my.key", attr.ConsumerTag);
		}

		[Fact]
		public void RoutingKey_DefaultsToWildcard()
		{
			var attr = new HandlerAttribute(ExchangeType.direct, "ex", "q");
			Assert.Equal("*", attr.RoutingKey);
		}

		[Fact]
		public void ExchangeType_IsMappedToString()
		{
			var attr = new HandlerAttribute(ExchangeType.topic, "ex", "q");
			Assert.Equal("topic", attr.ExchangeType);
		}

		[Fact]
		public void ExchangeName_IsPreserved()
		{
			var attr = new HandlerAttribute(ExchangeType.fanout, "fanout.exchange", "q");
			Assert.Equal("fanout.exchange", attr.ExchangeName);
		}

		[Fact]
		public void QueueName_IsPreserved()
		{
			var attr = new HandlerAttribute(ExchangeType.direct, "ex", "important.queue");
			Assert.Equal("important.queue", attr.QueueName);
		}
	}
}
