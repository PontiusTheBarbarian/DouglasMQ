using System;
using System.Threading.Tasks;
using DouglasMQ.Serialization;
using Testcontainers.RabbitMq;
using Xunit;

namespace DouglasMQ.Integration.Tests
{
	/// <summary>
	/// xUnit collection fixture that starts a single RabbitMQ container shared across
	/// all integration tests in the collection. Requires Docker to be running.
	/// </summary>
	public sealed class RabbitMqFixture : IAsyncLifetime
	{
		private readonly RabbitMqContainer _container = new RabbitMqBuilder()
			.WithUsername("guest")
			.WithPassword("guest")
			.Build();

		public string HostName => _container.Hostname;
		public int Port => _container.GetMappedPublicPort(5672);

		public DouglasMQBuilder CreateBuilder() =>
			new DouglasMQBuilder()
				.Configure(c =>
				{
					c.HostName = HostName;
					c.Username = "guest";
					c.Password = "guest";
					c.RetryCount = 2;
					c.RetryDelayMs = 50;
					c.PrefetchCount = 10;
				})
				.WithSerializer(new JsonSerializer());

		public async Task InitializeAsync() => await _container.StartAsync();

		public async Task DisposeAsync() => await _container.DisposeAsync();
	}

	[CollectionDefinition(CollectionName)]
	public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>
	{
		public const string CollectionName = "RabbitMQ";
	}

	/// <summary>
	/// Minimal JSON serializer used in integration tests.
	/// </summary>
	internal sealed class JsonSerializer : ISerializer
	{
		public object? Deserialize(string data, Type type) =>
			System.Text.Json.JsonSerializer.Deserialize(data, type);

		public string Serialize<T>(T data) =>
			System.Text.Json.JsonSerializer.Serialize(data);
	}
}
