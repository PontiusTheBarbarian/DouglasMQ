using System;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class PublisherConnectionOwnershipTests
	{
		private static Mock<IConnection> ConnectionWithChannel()
		{
			var channel = new Mock<IChannel>();
			channel.SetupGet(c => c.IsOpen).Returns(true);

			var connection = new Mock<IConnection>();
			connection.SetupGet(c => c.IsOpen).Returns(false);
			connection
				.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(channel.Object);
			return connection;
		}

		[Fact]
		public async Task OwnedConnection_IsDisposedWithPublisher()
		{
			// BuildPublisherAsync hands the publisher its own connection; it used to leak.
			var connection = new Mock<IConnection>();
			var publisher = new Publisher(connection.Object, ownsConnection: true, new Serializer(), NullLogger<Publisher>.Instance, new Configuration());

			await publisher.DisposeAsync();

			connection.Verify(c => c.DisposeAsync(), Times.Once);
		}

		[Fact]
		public async Task CallerOwnedConnection_IsLeftOpen()
		{
			var connection = new Mock<IConnection>();
			var publisher = new Publisher(connection.Object, new Serializer(), NullLogger<Publisher>.Instance, new Configuration());

			await publisher.DisposeAsync();

			connection.Verify(c => c.DisposeAsync(), Times.Never);
		}

		[Fact]
		public async Task LazyConnection_OpensOnFirstPublishOnly_AndIsDisposedWithPublisher()
		{
			var connection = ConnectionWithChannel();
			var opens = 0;
			var publisher = Publisher.WithLazyConnection(
				_ =>
				{
					opens++;
					return Task.FromResult(connection.Object);
				},
				new Serializer(),
				NullLogger<Publisher>.Instance,
				new Configuration());

			Assert.Equal(0, opens); // constructing (i.e. resolving from DI) must not touch the network

			await publisher.PublishAsync(new Event());
			await publisher.PublishAsync(new Event());
			Assert.Equal(1, opens);

			await publisher.DisposeAsync();
			connection.Verify(c => c.DisposeAsync(), Times.Once);
		}

		[Fact]
		public async Task LazyConnection_FailedOpen_IsRetriedOnNextPublish()
		{
			var connection = ConnectionWithChannel();
			var attempts = 0;
			var publisher = Publisher.WithLazyConnection(
				_ => ++attempts == 1
					? Task.FromException<IConnection>(new InvalidOperationException("broker down"))
					: Task.FromResult(connection.Object),
				new Serializer(),
				NullLogger<Publisher>.Instance,
				new Configuration());

			await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(new Event()).AsTask());
			await publisher.PublishAsync(new Event());

			Assert.Equal(2, attempts);
		}

		[Message("ownership.exchange", "ownership.key")]
		public sealed class Event : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		private sealed class Serializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => null;

			public string Serialize<T>(T data) => "{}";
		}
	}
}
