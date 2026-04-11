using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class MessageAttributeTests
	{
		[Fact]
		public void ExchangeName_IsPreserved()
		{
			var attr = new MessageAttribute("my.exchange");
			Assert.Equal("my.exchange", attr.ExchangeName);
		}

		[Fact]
		public void RoutingKey_DefaultsToWildcard()
		{
			var attr = new MessageAttribute("my.exchange");
			Assert.Equal("*", attr.RoutingKey);
		}

		[Fact]
		public void RoutingKey_IsPreservedWhenProvided()
		{
			var attr = new MessageAttribute("my.exchange", "my.key");
			Assert.Equal("my.key", attr.RoutingKey);
		}
	}
}
