using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;

namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Holds the activity source, meter and instruments DouglasMQ records telemetry with, plus
	/// helpers for propagating trace context through AMQP message headers.
	/// </summary>
	internal static class DouglasMQTelemetry
	{
		private static readonly string? Version = typeof(DouglasMQTelemetry).Assembly.GetName().Version?.ToString();

		public static readonly ActivitySource ActivitySource = new(DouglasMQInstrumentation.ActivitySourceName, Version);

		public static readonly Meter Meter = new(DouglasMQInstrumentation.MeterName, Version);

		// Bucket boundaries (seconds) recommended by the OpenTelemetry semantic conventions for durations.
		private static readonly InstrumentAdvice<double> DurationAdvice = new()
		{
			HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10],
		};

		public static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
			"douglasmq.messages.published",
			unit: "{message}",
			description: "Number of messages published to the broker.");

		public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
			"douglasmq.publish.duration",
			unit: "s",
			description: "Time taken to publish a message, including the broker confirm when publisher confirms are enabled.",
			tags: null,
			advice: DurationAdvice);

		public static readonly Counter<long> MessagesConsumed = Meter.CreateCounter<long>(
			"douglasmq.messages.consumed",
			unit: "{message}",
			description: "Number of messages received and processed, tagged with the final outcome.");

		public static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram<double>(
			"douglasmq.process.duration",
			unit: "s",
			description: "Time taken to process a received message, including all retry attempts.",
			tags: null,
			advice: DurationAdvice);

		public static readonly Counter<long> HandlerRetries = Meter.CreateCounter<long>(
			"douglasmq.handler.retries",
			unit: "{retry}",
			description: "Number of handler retry attempts.");

		public static readonly Counter<long> MessagesDeadLettered = Meter.CreateCounter<long>(
			"douglasmq.messages.dead_lettered",
			unit: "{message}",
			description: "Number of messages rejected to the dead-letter exchange.");

		/// <summary>
		/// Writes the trace context of <paramref name="activity"/> into AMQP headers.
		/// </summary>
		public static void InjectContext(Activity? activity, IDictionary<string, object?> headers)
		{
			if (activity is null)
			{
				return;
			}

			DistributedContextPropagator.Current.Inject(activity, headers, static (carrier, key, value) =>
			{
				if (carrier is IDictionary<string, object?> dictionary)
				{
					dictionary[key] = value;
				}
			});
		}

		/// <summary>
		/// Reads a remote parent context from AMQP headers. Returns <c>default</c> when none is present.
		/// </summary>
		public static ActivityContext ExtractContext(IDictionary<string, object?>? headers)
		{
			if (headers is null || headers.Count == 0)
			{
				return default;
			}

			DistributedContextPropagator.Current.ExtractTraceIdAndState(headers, GetHeader, out var traceParent, out var traceState);
			return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var context) ? context : default;
		}

		/// <summary>
		/// Reads W3C baggage from AMQP headers.
		/// </summary>
		public static IEnumerable<KeyValuePair<string, string?>> ExtractBaggage(IDictionary<string, object?>? headers)
		{
			if (headers is null || headers.Count == 0)
			{
				return Enumerable.Empty<KeyValuePair<string, string?>>();
			}

			return DistributedContextPropagator.Current.ExtractBaggage(headers, GetHeader)
				?? Enumerable.Empty<KeyValuePair<string, string?>>();
		}

		private static void GetHeader(object? carrier, string name, out string? value, out IEnumerable<string>? values)
		{
			values = null;
			value = null;

			if (carrier is IDictionary<string, object?> dictionary && dictionary.TryGetValue(name, out var raw))
			{
				// RabbitMQ.Client delivers string header values as UTF-8 byte arrays.
				value = raw switch
				{
					byte[] bytes => Encoding.UTF8.GetString(bytes),
					string text => text,
					_ => raw?.ToString(),
				};
			}
		}
	}
}
