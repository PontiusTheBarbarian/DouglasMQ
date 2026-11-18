using System;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class ConnectionFactoryBuilderTests
	{
		[Fact]
		public void MapsBasicSettings()
		{
			var factory = ConnectionFactoryBuilder.Create(new Configuration
			{
				HostName = "rabbit.internal",
				Port = 5673,
				Username = "app",
				Password = "secret",
				VirtualHost = "/orders",
			});

			Assert.Equal("rabbit.internal", factory.HostName);
			Assert.Equal(5673, factory.Port);
			Assert.Equal("app", factory.UserName);
			Assert.Equal("secret", factory.Password);
			Assert.Equal("/orders", factory.VirtualHost);
			Assert.False(factory.Ssl.Enabled);
		}

		[Fact]
		public void UseSsl_WithoutClientCertificate_StillEnablesTls()
		{
			// Previously TLS was silently skipped unless a client certificate was also supplied.
			var factory = ConnectionFactoryBuilder.Create(new Configuration { HostName = "rabbit.internal", UseSsl = true });

			Assert.True(factory.Ssl.Enabled);
			Assert.Equal("rabbit.internal", factory.Ssl.ServerName);
			Assert.True(factory.Ssl.Certs is null || factory.Ssl.Certs.Count == 0);
		}

		[Fact]
		public void UseSsl_ValidatesServerCertificateByDefault()
		{
			// Previously name-mismatch and chain errors were always accepted.
			var factory = ConnectionFactoryBuilder.Create(new Configuration { UseSsl = true });

			Assert.Equal(SslPolicyErrors.None, factory.Ssl.AcceptablePolicyErrors);
		}

		[Fact]
		public void UseSsl_HonoursExplicitServerNameAndRelaxedPolicy()
		{
			var factory = ConnectionFactoryBuilder.Create(new Configuration
			{
				HostName = "10.0.0.5",
				UseSsl = true,
				SslServerName = "rabbit.internal",
				SslAcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
			});

			Assert.Equal("rabbit.internal", factory.Ssl.ServerName);
			Assert.Equal(SslPolicyErrors.RemoteCertificateChainErrors, factory.Ssl.AcceptablePolicyErrors);
		}

		[Fact]
		public void UseSsl_Pkcs12ClientCertificate_KeepsPrivateKey()
		{
			// Mutual TLS needs the private key; the old loader dropped it.
			using var key = RSA.Create(2048);
			var request = new CertificateRequest("CN=douglasmq-test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
			using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
			var pfx = certificate.Export(X509ContentType.Pkcs12, "p@ss");

			var factory = ConnectionFactoryBuilder.Create(new Configuration
			{
				UseSsl = true,
				SslCertificateThumbprint = Convert.ToBase64String(pfx),
				SslCertificatePassword = "p@ss",
			});

			var loaded = Assert.IsType<X509Certificate2>(Assert.Single(factory.Ssl.Certs!));
			Assert.True(loaded.HasPrivateKey);
		}

		[Fact]
		public void UseSsl_InvalidBase64Certificate_ThrowsClearError()
		{
			var ex = Assert.Throws<ArgumentException>(() => ConnectionFactoryBuilder.Create(new Configuration
			{
				UseSsl = true,
				SslCertificateThumbprint = "not base64!",
			}));

			Assert.Contains(nameof(Configuration.SslCertificateThumbprint), ex.Message);
		}
	}
}
