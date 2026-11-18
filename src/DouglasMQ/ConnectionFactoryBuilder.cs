using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace DouglasMQ
{
	/// <summary>
	/// Single place that turns a <see cref="Configuration"/> into a RabbitMQ connection, shared by
	/// <see cref="DouglasMQBuilder"/> and the Microsoft DI registration so both behave identically.
	/// </summary>
	internal static class ConnectionFactoryBuilder
	{
		/// <summary>Suffix appended to the client-provided name of the publishing connection.</summary>
		public const string PublisherSuffix = "publisher";

		/// <summary>Suffix appended to the client-provided name of the consuming connection.</summary>
		public const string ConsumerSuffix = "consumer";

		/// <summary>
		/// Builds a <see cref="ConnectionFactory"/> from <paramref name="configuration"/>.
		/// </summary>
		/// <exception cref="ArgumentException">The client certificate cannot be decoded.</exception>
		public static ConnectionFactory Create(Configuration configuration)
		{
			ArgumentNullException.ThrowIfNull(configuration);

			var factory = new ConnectionFactory
			{
				UserName = configuration.Username,
				Password = configuration.Password,
				VirtualHost = configuration.VirtualHost,
				ClientProvidedName = configuration.ClientProvidedName,
				HostName = configuration.HostName,
				Port = configuration.Port,
				AutomaticRecoveryEnabled = true,
				TopologyRecoveryEnabled = true,
				RequestedHeartbeat = TimeSpan.FromSeconds(60),
			};

			if (configuration.UseSsl)
			{
				// TLS is enabled whenever UseSsl is set — a client certificate is optional (only
				// needed for mutual TLS). Server certificate validation stays on unless the caller
				// explicitly relaxes it.
				factory.Ssl = new SslOption
				{
					Enabled = true,
					ServerName = configuration.SslServerName ?? configuration.HostName,
					AcceptablePolicyErrors = configuration.SslAcceptablePolicyErrors,
				};

				if (!string.IsNullOrWhiteSpace(configuration.SslCertificateThumbprint))
				{
					factory.Ssl.Certs = new X509CertificateCollection
					{
						LoadClientCertificate(configuration.SslCertificateThumbprint, configuration.SslCertificatePassword),
					};
				}
			}

			return factory;
		}

		/// <summary>
		/// Opens a connection, tagging its client-provided name with <paramref name="purpose"/> so
		/// publisher and consumer connections are distinguishable in the management UI.
		/// </summary>
		public static Task<IConnection> CreateConnectionAsync(Configuration configuration, string? purpose, CancellationToken cancellationToken)
		{
			var factory = Create(configuration);
			var name = purpose is null ? configuration.ClientProvidedName : $"{configuration.ClientProvidedName}:{purpose}";
			return factory.CreateConnectionAsync(name, cancellationToken);
		}

		private static X509Certificate2 LoadClientCertificate(string base64, string? password)
		{
			byte[] bytes;
			try
			{
				bytes = Convert.FromBase64String(base64);
			}
			catch (FormatException ex)
			{
				throw new ArgumentException($"{nameof(Configuration.SslCertificateThumbprint)} must be a Base64-encoded certificate.", ex);
			}

			// A client certificate is only useful for mutual TLS if it carries its private key,
			// which means PKCS#12. LoadCertificate would silently drop the key.
			return X509Certificate2.GetCertContentType(bytes) == X509ContentType.Pkcs12
				? X509CertificateLoader.LoadPkcs12(bytes, password)
				: X509CertificateLoader.LoadCertificate(bytes);
		}
	}
}
