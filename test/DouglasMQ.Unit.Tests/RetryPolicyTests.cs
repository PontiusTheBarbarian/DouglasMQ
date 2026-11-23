using System;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class RetryPolicyTests
	{
		[Theory]
		[InlineData(1000, 1, 1000)]
		[InlineData(1000, 2, 2000)]
		[InlineData(1000, 4, 8000)]
		[InlineData(int.MaxValue, 40, 300_000)] // capped at 5 minutes instead of overflowing
		public void GetDelay_IsExponentialAndCapped(int baseDelayMs, int retry, double expectedMs)
		{
			Assert.Equal(expectedMs, new RetryPolicy(3, baseDelayMs).GetDelay(retry).TotalMilliseconds);
		}

		[Fact]
		public void NegativeSettings_AreTreatedAsZero()
		{
			var policy = new RetryPolicy(-1, -500);

			Assert.Equal(0, policy.RetryCount);
			Assert.Equal(1, policy.MaxAttempts);
			Assert.Equal(TimeSpan.Zero, policy.GetDelay(1));
		}

		[Fact]
		public void From_ReadsConfiguration()
		{
			var policy = RetryPolicy.From(new Configuration { RetryCount = 5, RetryDelayMs = 10 });

			Assert.Equal(5, policy.RetryCount);
			Assert.Equal(TimeSpan.FromMilliseconds(20), policy.GetDelay(2));
		}
	}
}
