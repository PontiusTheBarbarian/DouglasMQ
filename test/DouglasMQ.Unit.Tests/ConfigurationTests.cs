using System.Collections.Generic;
using DouglasMQ.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	public class ConfigurationTests
	{
		[Fact]
		public void Default_ClientProvidedName_IsNotNull()
		{
			var config = new Configuration();
			Assert.NotNull(config.ClientProvidedName);
		}

		[Fact]
		public void Default_ClientProvidedName_FallsBackWhenEntryAssemblyIsNull()
		{
			// Simulates a context where GetEntryAssembly() returns null (e.g. test runner).
			// The default is evaluated at field init time; we just verify it's never null/empty.
			var config = new Configuration();
			Assert.False(string.IsNullOrWhiteSpace(config.ClientProvidedName));
		}

		[Fact]
		public void Default_Username_IsGuest()
		{
			Assert.Equal("guest", new Configuration().Username);
		}

		[Fact]
		public void Default_Password_IsGuest()
		{
			Assert.Equal("guest", new Configuration().Password);
		}

		[Fact]
		public void Default_VirtualHost_IsSlash()
		{
			Assert.Equal("/", new Configuration().VirtualHost);
		}

		[Fact]
		public void Default_HostName_IsLocalhost()
		{
			Assert.Equal("localhost", new Configuration().HostName);
		}

		[Fact]
		public void Default_UseSsl_IsFalse()
		{
			Assert.False(new Configuration().UseSsl);
		}

		[Fact]
		public void Default_SslClientCertificate_IsNull()
		{
			Assert.Null(new Configuration().SslClientCertificate);
		}

		[Fact]
		public void Default_PrefetchCount_Is10()
		{
			Assert.Equal(10, new Configuration().PrefetchCount);
		}

		[Fact]
		public void Default_RetryCount_Is3()
		{
			Assert.Equal(3, new Configuration().RetryCount);
		}

		[Fact]
		public void Default_RetryDelayMs_Is1000()
		{
			Assert.Equal(1000, new Configuration().RetryDelayMs);
		}

		[Fact]
		public void Default_PublisherConfirms_IsTrue()
		{
			Assert.True(new Configuration().PublisherConfirms);
		}

		[Theory]
		[InlineData("SslClientCertificate")]
		[InlineData("SslCertificateThumbprint")] // old key in existing appsettings files
		public void ClientCertificate_BindsFromEitherConfigurationKey(string key)
		{
			var configuration = new ConfigurationBuilder()
				.AddInMemoryCollection(new Dictionary<string, string?> { [$"DouglasMQ:{key}"] = "Y2VydA==" })
				.Build();

			var provider = new ServiceCollection()
				.AddLogging()
				.AddDouglasMQ(configuration, typeof(SystemTextJsonSerializer))
				.BuildServiceProvider();

			Assert.Equal("Y2VydA==", provider.GetRequiredService<Configuration>().SslClientCertificate);
		}
	}
}
