using System;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class DouglasMQBuilderTests
	{
		[Fact]
		public void Configure_ReturnsSameBuilder()
		{
			var builder = new DouglasMQBuilder();
			var result = builder.Configure(c => c.HostName = "rabbit");
			Assert.Same(builder, result);
		}

		[Fact]
		public void WithSerializer_ReturnsSameBuilder()
		{
			var builder = new DouglasMQBuilder();
			var result = builder.WithSerializer(new FakeSerializer());
			Assert.Same(builder, result);
		}

		[Fact]
		public void WithSerializer_ThrowsOnNull()
		{
			var builder = new DouglasMQBuilder();
			Assert.Throws<ArgumentNullException>(() => builder.WithSerializer(null!));
		}

		[Fact]
		public void AddHandler_ThrowsOnNull()
		{
			var builder = new DouglasMQBuilder();
			Assert.Throws<ArgumentNullException>(() => builder.AddHandler(null!));
		}

		[Fact]
		public void AddHandler_ThrowsWhenHandlerAttributeMissing()
		{
			var builder = new DouglasMQBuilder();
			Assert.Throws<InvalidOperationException>(() => builder.AddHandler(new HandlerWithoutAttribute()));
		}

		[Fact]
		public void AddHandler_AcceptsDecoratedHandler()
		{
			new DouglasMQBuilder()
				.WithSerializer(new FakeSerializer())
				.AddHandler(new ValidHandler());

			// No exception thrown
		}

		[Fact]
		public void Serializer_DefaultsToSystemTextJson()
		{
			Assert.IsType<SystemTextJsonSerializer>(new DouglasMQBuilder().Serializer);
		}

		[Fact]
		public void WithSerializer_ReplacesDefault()
		{
			var custom = new FakeSerializer();
			Assert.Same(custom, new DouglasMQBuilder().WithSerializer(custom).Serializer);
		}

		// --- Test doubles ---

		public class HandlerWithoutAttribute : IHandler<SomeMessage>
		{
			public Task HandleAsync(SomeMessage message) => Task.CompletedTask;
		}

		[Handler(ExchangeType.direct, "ex", "q", "key")]
		public class ValidHandler : IHandler<SomeMessage>
		{
			public Task HandleAsync(SomeMessage message) => Task.CompletedTask;
		}

		[Message("ex", "key")]
		public class SomeMessage : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
		}

		private class FakeSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => null;
			public string Serialize<T>(T data) => "{}";
		}
	}
}
