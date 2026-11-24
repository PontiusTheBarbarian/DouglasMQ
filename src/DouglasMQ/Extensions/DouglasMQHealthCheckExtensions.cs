using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DouglasMQ
{
	/// <summary>
	/// Health checks for DouglasMQ, for use with <c>services.AddHealthChecks()</c>.
	/// </summary>
	public static class DouglasMQHealthCheckExtensions
	{
		/// <summary>The default name of the check added by <see cref="AddDouglasMQPublisherCheck"/>.</summary>
		public const string PublisherCheckName = "douglasmq-publisher";

		/// <summary>The default name of the check added by <see cref="AddDouglasMQConsumersCheck"/>.</summary>
		public const string ConsumersCheckName = "douglasmq-consumers";

		/// <summary>
		/// Adds a check that the publisher registered by <c>AddDouglasMQPublisher</c> or <c>AddDouglasMQ</c>
		/// can reach the broker. It opens the publisher's connection and channel if they are not open yet
		/// (so a probe also reconnects the publisher after an outage) and reports <paramref name="failureStatus"/>
		/// if that fails.
		/// </summary>
		/// <param name="builder">The health checks builder.</param>
		/// <param name="name">The check's name. Defaults to <see cref="PublisherCheckName"/>.</param>
		/// <param name="failureStatus">Status reported when the broker cannot be reached. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
		/// <param name="tags">Tags for filtering, e.g. <c>"ready"</c>.</param>
		/// <param name="timeout">How long a check may take. Defaults to 5 seconds.</param>
		public static IHealthChecksBuilder AddDouglasMQPublisherCheck(this IHealthChecksBuilder builder,
			string name = PublisherCheckName,
			HealthStatus? failureStatus = null,
			IEnumerable<string>? tags = null,
			TimeSpan? timeout = null)
		{
			ArgumentNullException.ThrowIfNull(builder);

			return builder.Add(new HealthCheckRegistration(
				name,
				provider => new PublisherHealthCheck(provider.GetRequiredService<Publisher>()),
				failureStatus,
				tags,
				timeout ?? TimeSpan.FromSeconds(5)));
		}

		/// <summary>
		/// Adds a check that reports <paramref name="failureStatus"/> until the consumers started by
		/// <c>AddDouglasMQ</c> are running (see <see cref="IDouglasMQStatus"/>). Opens no connection of its own.
		/// </summary>
		/// <param name="builder">The health checks builder.</param>
		/// <param name="name">The check's name. Defaults to <see cref="ConsumersCheckName"/>.</param>
		/// <param name="failureStatus">Status reported while the consumers are not running. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
		/// <param name="tags">Tags for filtering, e.g. <c>"ready"</c>.</param>
		public static IHealthChecksBuilder AddDouglasMQConsumersCheck(this IHealthChecksBuilder builder,
			string name = ConsumersCheckName,
			HealthStatus? failureStatus = null,
			IEnumerable<string>? tags = null)
		{
			ArgumentNullException.ThrowIfNull(builder);

			return builder.Add(new HealthCheckRegistration(
				name,
				provider => new ConsumersHealthCheck(provider.GetRequiredService<IDouglasMQStatus>()),
				failureStatus,
				tags));
		}

		private sealed class PublisherHealthCheck : IHealthCheck
		{
			private readonly Publisher _publisher;

			public PublisherHealthCheck(Publisher publisher) => _publisher = publisher;

			public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
			{
				try
				{
					await _publisher.ConnectAsync(cancellationToken);
					return HealthCheckResult.Healthy("The DouglasMQ publisher is connected to the broker.");
				}
				catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
				{
					return new HealthCheckResult(context.Registration.FailureStatus, "The DouglasMQ publisher cannot reach the broker.", ex);
				}
			}
		}

		private sealed class ConsumersHealthCheck : IHealthCheck
		{
			private readonly IDouglasMQStatus _status;

			public ConsumersHealthCheck(IDouglasMQStatus status) => _status = status;

			public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
				=> Task.FromResult(_status.ConsumersRunning
					? HealthCheckResult.Healthy("The DouglasMQ consumers are running.")
					: new HealthCheckResult(context.Registration.FailureStatus, "The DouglasMQ consumers are not running yet."));
		}
	}
}
