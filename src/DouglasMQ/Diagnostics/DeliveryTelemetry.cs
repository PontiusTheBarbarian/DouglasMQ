using System.Diagnostics;

namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Per-delivery state collected while processing, then written once to the delivery's
	/// <c>process {queue}</c> span and to the consumed / dead-lettered / duration metrics.
	/// </summary>
	internal sealed class DeliveryTelemetry
	{
		/// <summary>Telemetry for a delivery with no span (nothing is listening, or a test).</summary>
		public static DeliveryTelemetry None => new(null);

		public DeliveryTelemetry(Activity? activity) => Activity = activity;

		/// <summary>The delivery span. Per-attempt spans are its children.</summary>
		public Activity? Activity { get; }

		/// <summary><c>ack</c>, <c>dead_lettered</c> or <c>requeued</c>.</summary>
		public string? Outcome { get; set; }

		/// <summary>The exception type that caused dead-lettering, if any.</summary>
		public string? ErrorType { get; set; }

		/// <summary>Retries used (attempts after the first).</summary>
		public int Retries { get; set; }
	}
}
