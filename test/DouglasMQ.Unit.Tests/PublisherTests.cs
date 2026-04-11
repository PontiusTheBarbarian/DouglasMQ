using System;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class PublisherTests
	{
		private static Publisher CreatePublisher(IConnection connection, ISerializer? serializer = null, Configuration? config = null)
		{
			return new Publisher(
				connection,
				serializer ?? new FakeSerializer(),
				NullLogger<Publisher>.Instance,
				config ?? new Configuration());
		}

		[Fact]
		public void Constructor_ThrowsOnNullConnection()
		{
			Assert.Throws<ArgumentNullException>(() =>
				new Publisher(null!, new FakeSerializer(), NullLogger<Publisher>.Instance, new Configuration()));
		}

		[Fact]
		public void Constructor_ThrowsOnNullSerializer()
		{
			var connection = new Mock<IConnection>().Object;
			Assert.Throws<ArgumentNullException>(() =>
				new Publisher(connection, null!, NullLogger<Publisher>.Instance, new Configuration()));
		}

		[Fact]
		public void Constructor_ThrowsOnNullLogger()
		{
			var connection = new Mock<IConnection>().Object;
			Assert.Throws<ArgumentNullException>(() =>
				new Publisher(connection, new FakeSerializer(), null!, new Configuration()));
		}

		[Fact]
		public async Task PublishAsync_ThrowsOnNullEvent()
		{
			var connection = new Mock<IConnection>().Object;
			var publisher = CreatePublisher(connection);
			await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync<TestEvent>(null!).AsTask());
		}

		[Fact]
		public async Task SendAsync_ThrowsOnNullCommand()
		{
			var connection = new Mock<IConnection>().Object;
			var publisher = CreatePublisher(connection);
			await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.SendAsync<TestCommand>(null!).AsTask());
		}

		[Fact]
		public async Task PublishAsync_ThrowsWhenMessageAttributeMissing()
		{
			var connection = new Mock<IConnection>().Object;
			var publisher = CreatePublisher(connection);
			await Assert.ThrowsAsync<InvalidOperationException>(() =>
				publisher.PublishAsync(new EventWithoutAttribute()).AsTask());
		}

		[Fact]
		public async Task SendAsync_ThrowsWhenMessageAttributeMissing()
		{
			var connection = new Mock<IConnection>().Object;
			var publisher = CreatePublisher(connection);
			await Assert.ThrowsAsync<InvalidOperationException>(() =>
				publisher.SendAsync(new CommandWithoutAttribute()).AsTask());
		}

		// --- Test doubles ---

		[Message("test.exchange", "test.key")]
		public class TestEvent : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Message("test.exchange", "test.key")]
		public class TestCommand : ICommand
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
		}

		public class EventWithoutAttribute : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
		}

		public class CommandWithoutAttribute : ICommand
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
