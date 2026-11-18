using System;
using System.Text.Json;
using DouglasMQ.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class SystemTextJsonSerializerTests
	{
		private readonly SystemTextJsonSerializer _serializer = new();

		[Fact]
		public void RoundTrips()
		{
			var original = new Order { OrderId = "ORD-1", Quantity = 3 };

			var result = Assert.IsType<Order>(_serializer.Deserialize(_serializer.Serialize(original), typeof(Order)));

			Assert.Equal(original.MessageId, result.MessageId);
			Assert.Equal("ORD-1", result.OrderId);
			Assert.Equal(3, result.Quantity);
		}

		[Fact]
		public void UsesCamelCaseOnTheWire_AndReadsCaseInsensitively()
		{
			var json = _serializer.Serialize(new Order { OrderId = "ORD-1" });
			Assert.Contains("\"orderId\":\"ORD-1\"", json);

			var result = Assert.IsType<Order>(_serializer.Deserialize("""{"ORDERID":"ORD-2"}""", typeof(Order)));
			Assert.Equal("ORD-2", result.OrderId);
		}

		[Fact]
		public void CustomOptions_AreHonoured()
		{
			var serializer = new SystemTextJsonSerializer(new JsonSerializerOptions());

			Assert.Contains("\"OrderId\"", serializer.Serialize(new Order { OrderId = "x" }));
		}

		[Fact]
		public void MalformedJson_Throws()
		{
			// The consumer relies on this to dead-letter poison messages.
			Assert.ThrowsAny<JsonException>(() => _serializer.Deserialize("{ nope", typeof(Order)));
		}

		[Fact]
		public void DiRegistration_DefaultsToSystemTextJson_ButYieldsToAppSerializer()
		{
			var defaulted = new ServiceCollection();
			defaulted.AddDouglasMQ((_, _) => { });
			Assert.IsType<SystemTextJsonSerializer>(defaulted.BuildServiceProvider().GetRequiredService<ISerializer>());

			var registeredBefore = new ServiceCollection();
			registeredBefore.AddSingleton<ISerializer, CustomSerializer>();
			registeredBefore.AddDouglasMQ((_, _) => { });
			Assert.IsType<CustomSerializer>(registeredBefore.BuildServiceProvider().GetRequiredService<ISerializer>());

			var registeredAfter = new ServiceCollection();
			registeredAfter.AddDouglasMQ((_, _) => { });
			registeredAfter.AddSerializer(typeof(CustomSerializer));
			Assert.IsType<CustomSerializer>(registeredAfter.BuildServiceProvider().GetRequiredService<ISerializer>());
		}

		public sealed class Order : IEvent
		{
			public Guid MessageId { get; init; } = Guid.NewGuid();

			public Guid TraceId { get; init; } = Guid.NewGuid();

			public string OrderId { get; set; } = string.Empty;

			public int Quantity { get; set; }
		}

		private sealed class CustomSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => null;

			public string Serialize<T>(T data) => string.Empty;
		}
	}
}
