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
		public void ByteMembers_MatchStringMembers()
		{
			ISerializer serializer = _serializer;
			var order = new Order { OrderId = "ORD-1", Quantity = 2 };

			var bytes = serializer.SerializeToBytes(order);

			Assert.Equal(serializer.Serialize(order), System.Text.Encoding.UTF8.GetString(bytes));
			var result = Assert.IsType<Order>(serializer.DeserializeFromBytes(bytes, typeof(Order)));
			Assert.Equal("ORD-1", result.OrderId);
		}

		[Fact]
		public void StringOnlySerializer_GetsUtf8ByteMembersByDefault()
		{
			// Existing custom serializers that only implement the string members keep working.
			ISerializer serializer = new StringOnlySerializer();

			Assert.Equal("ünïcode"u8.ToArray(), serializer.SerializeToBytes(new Order()));
			Assert.Equal("ünïcode", serializer.DeserializeFromBytes("ünïcode"u8, typeof(string)));
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

		private sealed class StringOnlySerializer : ISerializer
		{
			public string Serialize<T>(T data) => "ünïcode";

			public object? Deserialize(string data, Type type) => data;
		}

		private sealed class CustomSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => null;

			public string Serialize<T>(T data) => string.Empty;
		}
	}
}
