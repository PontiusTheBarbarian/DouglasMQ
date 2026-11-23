using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// A broker outage must not stop the host from starting: consumers retry in the background
	/// so the app can report the outage (for example as a failing readiness probe).
	/// </summary>
	public class UnreachableBrokerHostTests
	{
		private static int ClosedPort()
		{
			var listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			var port = ((IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();
			return port;
		}

		[Fact]
		public async Task Host_StartsAndStopsCleanly_WhileTheBrokerIsUnreachable_AndLogsTheRetries()
		{
			var logger = new RecordingLogger();
			var builder = Host.CreateApplicationBuilder();
			builder.Logging.ClearProviders().AddProvider(logger.Provider).SetMinimumLevel(LogLevel.Debug);
			builder.Services.AddDouglasMQ((config, services) =>
			{
				config.HostName = "127.0.0.1";
				config.Port = ClosedPort();
				services.AddConsumer<Message, Handler>();
			});

			using var host = builder.Build();

			var startup = Stopwatch.StartNew();
			await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(5));
			Assert.True(startup.Elapsed < TimeSpan.FromSeconds(2), $"Host start took {startup.Elapsed}.");

			var status = host.Services.GetRequiredService<IDouglasMQStatus>();
			Assert.False(status.ConsumersRunning);

			// First failure immediately, the next after the 2 s initial delay.
			var deadline = Stopwatch.StartNew();
			while (logger.Entries.Count(IsRetryWarning) < 2)
			{
				Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(15), "Expected two start-up retry warnings.");
				await Task.Delay(50);
			}

			var retries = logger.Entries.Where(IsRetryWarning).ToList();
			Assert.All(retries, r => Assert.NotNull(r.Exception));
			Assert.Contains("00:00:02", retries[0].Message);
			Assert.Contains("00:00:04", retries[1].Message);

			var stopping = Stopwatch.StartNew();
			await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
			Assert.True(stopping.Elapsed < TimeSpan.FromSeconds(2), $"Host stop took {stopping.Elapsed}.");
			Assert.False(status.ConsumersRunning);
			Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Error);
		}

		private static bool IsRetryWarning(RecordingLogger.Entry entry)
			=> entry.Level == LogLevel.Warning && entry.Message.Contains("could not start its consumers", StringComparison.Ordinal);

		public sealed class Message : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "unreachable.exchange", "unreachable.queue")]
		public sealed class Handler : IHandler<Message>
		{
			public Task HandleAsync(Message message) => Task.CompletedTask;
		}
	}
}
