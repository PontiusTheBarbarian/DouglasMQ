using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// R9: a publisher that connects on first use can be registered without starting consumers.
	/// </summary>
	public class PublisherRegistrationTests
	{
		private static int ClosedPort()
		{
			var listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			var port = ((IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();
			return port;
		}

		private static IConfiguration UnreachableBroker() => new ConfigurationBuilder()
			.AddInMemoryCollection(new Dictionary<string, string?>
			{
				["DouglasMQ:HostName"] = "127.0.0.1",
				["DouglasMQ:Port"] = ClosedPort().ToString(System.Globalization.CultureInfo.InvariantCulture),
			})
			.Build();

		[Fact]
		public async Task AddDouglasMQPublisher_RegistersAPublisher_AndNoConsumers()
		{
			var builder = Host.CreateApplicationBuilder();
			builder.Services.AddDouglasMQPublisher(UnreachableBroker());
			using var host = builder.Build();

			// Starting the host and resolving the publisher must not touch the (unreachable) broker.
			await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(5));

			Assert.IsType<Publisher>(host.Services.GetRequiredService<IPublisher>());
			Assert.IsType<SystemTextJsonSerializer>(host.Services.GetRequiredService<ISerializer>());
			Assert.Null(host.Services.GetService<IDouglasMQStatus>());
			Assert.DoesNotContain(host.Services.GetServices<IHostedService>(), s => s is Dispatcher);

			await host.StopAsync();
		}

		[Fact]
		public void AddDouglasMQPublisher_BindsTheDouglasMQSection()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQPublisher(UnreachableBroker());
			using var provider = services.BuildServiceProvider();

			Assert.Equal("127.0.0.1", provider.GetRequiredService<Configuration>().HostName);
		}

		[Fact]
		public void AddDouglasMQPublisher_UsesTheGivenSerializer()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQPublisher(UnreachableBroker(), typeof(CustomSerializer));
			using var provider = services.BuildServiceProvider();

			Assert.IsType<CustomSerializer>(provider.GetRequiredService<ISerializer>());
		}

		[Fact]
		public async Task AddDouglasMQPublisher_WithAddDouglasMQ_RegistersOnePublisher()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQPublisher(c => c.HostName = "127.0.0.1");
			services.AddDouglasMQ((c, _) => c.HostName = "127.0.0.1");
			await using var provider = services.BuildServiceProvider();

			Assert.Single(services, d => d.ServiceType == typeof(IPublisher));
			Assert.Same(provider.GetRequiredService<Publisher>(), provider.GetRequiredService<IPublisher>());
		}

		[Fact]
		public async Task PublisherCheck_IsUnhealthy_WhenTheBrokerIsUnreachable()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQPublisher(UnreachableBroker());
			services.AddHealthChecks().AddDouglasMQPublisherCheck(tags: ["ready"]);
			await using var provider = services.BuildServiceProvider();

			var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

			var entry = report.Entries[DouglasMQHealthCheckExtensions.PublisherCheckName];
			Assert.Equal(HealthStatus.Unhealthy, entry.Status);
			Assert.NotNull(entry.Exception);
			Assert.Contains("ready", entry.Tags);
		}

		[Fact]
		public async Task ConsumersCheck_IsUnhealthy_UntilTheConsumersRun()
		{
			var services = new ServiceCollection().AddLogging();
			services.AddDouglasMQ((c, _) =>
			{
				c.HostName = "127.0.0.1";
				c.Port = ClosedPort();
			});
			services.AddHealthChecks().AddDouglasMQConsumersCheck(failureStatus: HealthStatus.Degraded);
			await using var provider = services.BuildServiceProvider();

			var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

			Assert.Equal(HealthStatus.Degraded, report.Entries[DouglasMQHealthCheckExtensions.ConsumersCheckName].Status);
		}

		private sealed class CustomSerializer : ISerializer
		{
			private readonly SystemTextJsonSerializer _inner = new();

			public string Serialize<T>(T data) => _inner.Serialize(data);

			public object? Deserialize(string data, Type type) => _inner.Deserialize(data, type);
		}
	}
}
