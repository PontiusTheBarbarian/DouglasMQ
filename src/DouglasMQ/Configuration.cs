using System.Reflection;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Configuration settings for the DouglasMQ client.
	/// </summary>
	public class Configuration
	{
		/// <summary>
		/// The client-provided name sent to the broker. Defaults to the entry assembly name.
		/// </summary>
		public string ClientProvidedName { get; set; } = Assembly.GetEntryAssembly()?.FullName ?? "DouglasMQ";

		/// <summary>
		/// RabbitMQ username. Defaults to "guest".
		/// </summary>
		public string Username { get; set; } = "guest";

		/// <summary>
		/// RabbitMQ password. Defaults to "guest".
		/// </summary>
		public string Password { get; set; } = "guest";

		/// <summary>
		/// RabbitMQ virtual host. Defaults to "/".
		/// </summary>
		public string VirtualHost { get; set; } = "/";

		/// <summary>
		/// RabbitMQ broker hostname. Defaults to "localhost".
		/// </summary>
		public string HostName { get; set; } = "localhost";

		/// <summary>
		/// RabbitMQ broker port. Defaults to -1, which uses the protocol default
		/// (5672 for plain AMQP, 5671 when <see cref="UseSsl"/> is enabled).
		/// </summary>
		public int Port { get; set; } = AmqpTcpEndpoint.UseDefaultPort;

		/// <summary>
		/// Whether to enable SSL/TLS for the connection. Defaults to false.
		/// </summary>
		public bool UseSsl { get; set; } = false;

		/// <summary>
		/// Base64-encoded X509 certificate thumbprint used when <see cref="UseSsl"/> is true.
		/// </summary>
		public string? SslCertificateThumbprint { get; set; }

		/// <summary>
		/// Number of messages delivered to each consumer before requiring acknowledgements.
		/// Limits in-flight messages per consumer. Defaults to 10.
		/// </summary>
		public ushort PrefetchCount { get; set; } = 10;

		/// <summary>
		/// Number of times a failed message handler will be retried before the message is
		/// dead-lettered. Set to 0 to disable retries. Defaults to 3.
		/// </summary>
		public int RetryCount { get; set; } = 3;

		/// <summary>
		/// Base delay in milliseconds between retry attempts. Each attempt doubles this value
		/// (exponential backoff). Defaults to 1000 ms.
		/// </summary>
		public int RetryDelayMs { get; set; } = 1000;

		/// <summary>
		/// When true the publisher will wait for broker confirmation that each message has been
		/// persisted before returning. Has a performance cost. Defaults to false.
		/// </summary>
		public bool PublisherConfirms { get; set; } = false;
	}
}
