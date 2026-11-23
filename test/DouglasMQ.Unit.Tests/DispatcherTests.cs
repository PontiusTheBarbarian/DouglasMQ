using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class DispatcherTests
	{
		private readonly Mock<IConnection> _connection = new();
		private int _opens;

		private Dispatcher CreateDispatcher(params object[] handlers)
		{
			var registrations = new List<HandlerRegistration>();
			foreach (var handler in handlers)
			{
				registrations.Add(HandlerRegistration.For(HandlerInvoker.Create(handler)));
			}

			return new Dispatcher(
				_ =>
				{
					_opens++;
					return Task.FromResult(_connection.Object);
				},
				registrations,
				new SystemTextJsonSerializer(),
				new Configuration(),
				NullLoggerFactory.Instance);
		}

		private static Mock<IChannel> OpenChannel()
		{
			var channel = new Mock<IChannel>();
			channel.SetupGet(c => c.IsOpen).Returns(true);
			channel
				.Setup(c => c.QueueDeclareAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(new QueueDeclareOk("q", 0, 0));
			return channel;
		}

		[Fact]
		public void Constructing_DoesNotConnect()
		{
			CreateDispatcher(new HandlerA());

			Assert.Equal(0, _opens);
		}

		[Fact]
		public async Task StartupFailure_ReleasesChannelsAndConnection()
		{
			var dlxChannel = OpenChannel();
			var firstConsumerChannel = OpenChannel();
			_connection.SetupGet(c => c.IsOpen).Returns(true);
			_connection
				.SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(dlxChannel.Object)
				.ReturnsAsync(firstConsumerChannel.Object)
				.ThrowsAsync(new InvalidOperationException("broker refused channel"));

			var dispatcher = CreateDispatcher(new HandlerA(), new HandlerB());

			await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.StartAsync(CancellationToken.None));

			dlxChannel.Verify(c => c.DisposeAsync(), Times.Once);
			firstConsumerChannel.Verify(c => c.DisposeAsync(), Times.Once);
			_connection.Verify(c => c.DisposeAsync(), Times.Once);
		}

		[Fact]
		public async Task StopThenDispose_ClosesConnectionOnce()
		{
			_connection.SetupGet(c => c.IsOpen).Returns(true);
			_connection
				.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(() => OpenChannel().Object);

			var dispatcher = CreateDispatcher(new HandlerA());
			await dispatcher.StartAsync(CancellationToken.None);
			await dispatcher.StopAsync(CancellationToken.None);
			await dispatcher.DisposeAsync();

			Assert.Equal(1, _opens);
			_connection.Verify(c => c.DisposeAsync(), Times.Once);
		}

		public sealed class Message : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "dispatcher.exchange", "dispatcher.a")]
		private sealed class HandlerA : IHandler<Message>
		{
			public Task HandleAsync(Message message) => Task.CompletedTask;
		}

		[Handler(ExchangeType.direct, "dispatcher.exchange", "dispatcher.b")]
		private sealed class HandlerB : IHandler<Message>
		{
			public Task HandleAsync(Message message) => Task.CompletedTask;
		}
	}
}
