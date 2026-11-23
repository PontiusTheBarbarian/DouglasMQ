using System.Collections.Generic;
using DouglasMQ.Diagnostics;

namespace DouglasMQ
{
	/// <summary>
	/// The envelope of a delivery that processing needs for tracing: where it came from and its headers.
	/// Kept free of RabbitMQ.Client types so <see cref="DeliveryProcessor"/> stays broker-agnostic.
	/// </summary>
	/// <param name="Exchange">The exchange the message was published to.</param>
	/// <param name="RoutingKey">The routing key it was published with.</param>
	/// <param name="Headers">The AMQP headers, which may carry W3C trace context.</param>
	/// <param name="Telemetry">The delivery's span and the state recorded on it once processing finishes.</param>
	internal sealed record DeliveryInfo(string Exchange, string RoutingKey, IDictionary<string, object?>? Headers, DeliveryTelemetry Telemetry)
	{
		/// <summary>A delivery with no envelope information, for callers (and tests) that have none.</summary>
		public static DeliveryInfo Empty => new(string.Empty, string.Empty, null, DeliveryTelemetry.None);
	}
}
