using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class DispatcherTests
	{
		private readonly Mock<IConnection> _connection = new();
		private readonly List<Mock<IChannel>> _channels = new();
		private readonly RecordingLogger _logger = new();
		private Func<Task<IConnection>>? _openConnection;
		private int _opens;

		public DispatcherTests()
		{
			_connection.SetupGet(c => c.IsOpen).Returns(true);
			_connection
				.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(() =>
				{
					var channel = OpenChannel();
					lock (_channels)
					{
						_channels.Add(channel);
					}

					return channel.Object;
				});
		}

		private Dispatcher CreateDispatcher(params object[] handlers)
		{
			var registrations = handlers.Select(h => HandlerRegistration.For(HandlerInvoker.Create(h))).ToList();
			var loggerFactory = new LoggerFactory(new[] { _logger.Provider });

			return new Dispatcher(
				_ =>
				{
					Interlocked.Increment(ref _opens);
					return _openConnection?.Invoke() ?? Task.FromResult(_connection.Object);
				},
				registrations,
				new SystemTextJsonSerializer(),
				new Configuration(),
				loggerFactory)
			{
				InitialStartupRetryDelay = TimeSpan.FromMilliseconds(20),
				MaxStartupRetryDelay = TimeSpan.FromMilliseconds(80),
			};
		}

		private static Mock<IChannel> OpenChannel()
		{
			var channel = new Mock<IChannel>();
			var open = true;
			channel.SetupGet(c => c.IsOpen).Returns(() => open);
			channel.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Callback(() => open = false)
				.Returns(Task.CompletedTask);
			channel
				.Setup(c => c.QueueDeclareAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(new QueueDeclareOk("q", 0, 0));
			channel
				.Setup(c => c.BasicConsumeAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(), It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string _, bool _, string tag, bool _, bool _, IDictionary<string, object?> _, IAsyncBasicConsumer _, CancellationToken _) => tag);
			return channel;
		}

		private static async Task WaitUntilAsync(Func<bool> condition)
		{
			var deadline = Stopwatch.StartNew();
			while (!condition())
			{
				Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(5), "Condition not met within 5 s.");
				await Task.Delay(10);
			}
		}

		[Fact]
		public void Constructing_DoesNotConnect()
		{
			CreateDispatcher(new HandlerA());

			Assert.Equal(0, _opens);
		}

		[Fact]
		public async Task StartAndWait_Failure_ReleasesChannelsAndConnection()
		{
			var dlxChannel = OpenChannel();
			var firstConsumerChannel = OpenChannel();
			_connection
				.SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(dlxChannel.Object)
				.ReturnsAsync(firstConsumerChannel.Object)
				.ThrowsAsync(new InvalidOperationException("broker refused channel"));

			var dispatcher = CreateDispatcher(new HandlerA(), new HandlerB());

			await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.StartAndWaitAsync(CancellationToken.None));

			dlxChannel.Verify(c => c.DisposeAsync(), Times.Once);
			firstConsumerChannel.Verify(c => c.DisposeAsync(), Times.Once);
			_connection.Verify(c => c.DisposeAsync(), Times.Once);
			Assert.False(dispatcher.ConsumersRunning);
		}

		[Fact]
		public async Task HostedStart_ReturnsImmediately_AndRetriesWithGrowingDelayUntilTheBrokerIsUp()
		{
			var failures = 3;
			_openConnection = () => Interlocked.Decrement(ref failures) >= 0
				? Task.FromException<IConnection>(new InvalidOperationException("connection refused"))
				: Task.FromResult(_connection.Object);
			var dispatcher = CreateDispatcher(new HandlerA(), new HandlerB());

			var start = dispatcher.StartAsync(CancellationToken.None);

			Assert.True(start.IsCompletedSuccessfully);
			await WaitUntilAsync(() => dispatcher.ConsumersRunning);
			Assert.Equal(4, _opens);

			var warnings = _logger.Entries.Where(e => e.Level == LogLevel.Warning).ToList();
			Assert.Equal(3, warnings.Count);
			Assert.All(warnings, w => Assert.IsType<InvalidOperationException>(w.Exception));
			Assert.Contains("00:00:00.0200000", warnings[0].Message); // first delay
			Assert.Contains("00:00:00.0400000", warnings[1].Message); // doubled
			Assert.Contains("00:00:00.0800000", warnings[2].Message); // capped

			var started = Assert.Single(_logger.Entries, e => e.Message.Contains("consumers started", StringComparison.Ordinal));
			Assert.Equal(LogLevel.Information, started.Level);
			Assert.Contains("2 consumer(s)", started.Message);

			await dispatcher.StopAsync(CancellationToken.None);
			Assert.False(dispatcher.ConsumersRunning);
		}

		[Fact]
		public async Task Stop_WhileStartupKeepsFailing_EndsTheRetryLoopPromptly()
		{
			_openConnection = () => Task.FromException<IConnection>(new InvalidOperationException("connection refused"));
			var dispatcher = new Dispatcher(
				_ => _openConnection(),
				Array.Empty<HandlerRegistration>(),
				new SystemTextJsonSerializer(),
				new Configuration(),
				new LoggerFactory(new[] { _logger.Provider })); // default delays: 2 s, doubling to 1 min

			await dispatcher.StartAsync(CancellationToken.None);
			await WaitUntilAsync(() => _logger.Entries.Any(e => e.Level == LogLevel.Warning));

			var stopwatch = Stopwatch.StartNew();
			await dispatcher.StopAsync(CancellationToken.None);

			Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1), $"Stop took {stopwatch.Elapsed}.");
		}

		[Fact]
		public async Task OverlappingStopsAndDispose_CloseEachConsumerAndTheConnectionOnce()
		{
			var dispatcher = CreateDispatcher(new HandlerA(), new HandlerB());
			await dispatcher.StartAndWaitAsync(CancellationToken.None);

			await Task.WhenAll(
				dispatcher.StopAsync(CancellationToken.None),
				dispatcher.StopAsync(CancellationToken.None),
				dispatcher.DisposeAsync().AsTask());

			// One DLX channel plus one channel per consumer, each closed and disposed exactly once.
			Assert.Equal(3, _channels.Count);
			Assert.All(_channels, channel =>
			{
				channel.Verify(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
				channel.Verify(c => c.DisposeAsync(), Times.Once);
			});
			_connection.Verify(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
			_connection.Verify(c => c.DisposeAsync(), Times.Once);
		}

		[Fact]
		public async Task LaterStopCallers_WaitForTheFirstToFinish()
		{
			var dispatcher = CreateDispatcher(new HandlerA());
			await dispatcher.StartAndWaitAsync(CancellationToken.None);
			var closing = new TaskCompletionSource();
			_connection
				.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
				.Returns(closing.Task);

			var first = dispatcher.StopAsync(CancellationToken.None);
			var second = dispatcher.DisposeAsync().AsTask();
			await Task.Delay(50);

			Assert.False(first.IsCompleted);
			Assert.False(second.IsCompleted);

			closing.SetResult();
			await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));
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
