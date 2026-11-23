using System;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class MessageRouteTests
	{
		[Fact]
		public void Require_ReturnsTheMessageAttribute()
		{
			var route = MessageRoute<Routed>.Require();

			Assert.Equal("route.exchange", route.ExchangeName);
			Assert.Equal("route.key", route.RoutingKey);
			Assert.Same(route, MessageRoute<Routed>.Require()); // resolved once per type
		}

		[Fact]
		public void Require_IsInheritedFromBaseClass()
		{
			Assert.Equal("route.exchange", MessageRoute<DerivedRouted>.Require().ExchangeName);
		}

		[Fact]
		public void Require_ThrowsEveryTimeWhenAttributeMissing()
		{
			Assert.Throws<InvalidOperationException>(() => MessageRoute<Unrouted>.Require());
			var ex = Assert.Throws<InvalidOperationException>(() => MessageRoute<Unrouted>.Require());
			Assert.Contains(nameof(Unrouted), ex.Message);
		}

		[Fact]
		public void TypeName_IsTheFullName()
		{
			Assert.Equal(typeof(Routed).FullName, MessageRoute<Routed>.TypeName);
		}

		[Message("route.exchange", "route.key")]
		public class Routed
		{
		}

		public sealed class DerivedRouted : Routed
		{
		}

		public sealed class Unrouted
		{
		}
	}
}
