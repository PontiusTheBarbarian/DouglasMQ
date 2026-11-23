namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Names of the <see cref="System.Diagnostics.ActivitySource"/> and <see cref="System.Diagnostics.Metrics.Meter"/>
	/// that DouglasMQ emits telemetry through. Pass these to OpenTelemetry to collect DouglasMQ traces and metrics.
	/// </summary>
	/// <example>
	/// <code>
	/// services.AddOpenTelemetry()
	///     .WithTracing(t => t.AddSource(DouglasMQInstrumentation.ActivitySourceName))
	///     .WithMetrics(m => m.AddMeter(DouglasMQInstrumentation.MeterName));
	/// </code>
	/// </example>
	public static class DouglasMQInstrumentation
	{
		/// <summary>
		/// The name of the activity source used for publish and process spans.
		/// </summary>
		public const string ActivitySourceName = "DouglasMQ";

		/// <summary>
		/// The name of the meter used for DouglasMQ metrics.
		/// </summary>
		public const string MeterName = "DouglasMQ";
	}
}
