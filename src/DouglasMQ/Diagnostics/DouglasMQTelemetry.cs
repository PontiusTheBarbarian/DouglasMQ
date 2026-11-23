using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// The activity source DouglasMQ traces through, plus trace-context propagation through AMQP headers.
	/// </summary>
	internal static class DouglasMQTelemetry
	{
		private static readonly string? Version = typeof(DouglasMQTelemetry).Assembly.GetName().Version?.ToString();

		public static readonly ActivitySource ActivitySource = new(DouglasMQInstrumentation.ActivitySourceName, Version);

		/// <summary>
		/// Writes the trace context of <paramref name="activity"/> (<c>traceparent</c>, <c>tracestate</c>,
		/// <c>baggage</c>) into AMQP headers.
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
		/// A remote context in the trace identified by a message's <c>TraceId</c>, for messages published
		/// without a <c>traceparent</c> header. A <see cref="Guid"/> and a W3C trace id are both 16 bytes,
		/// so the trace id is the <c>TraceId</c> itself. The parent span id is derived from the
		/// <c>MessageId</c>, so every attempt at one message shares the same (synthetic) parent.
		/// </summary>
		public static ActivityContext FromMessageIds(Guid traceId, Guid messageId)
		{
			if (traceId == Guid.Empty)
			{
				return default;
			}

			Span<byte> messageBytes = stackalloc byte[16];
			messageId.TryWriteBytes(messageBytes);
			var spanBytes = messageBytes[..8];
			var spanId = spanBytes.IndexOfAnyExcept((byte)0) < 0
				? ActivitySpanId.CreateRandom()
				: ActivitySpanId.CreateFromBytes(spanBytes);

			return new ActivityContext(
				ActivityTraceId.CreateFromString(traceId.ToString("N")),
				spanId,
				ActivityTraceFlags.Recorded,
				isRemote: true);
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
