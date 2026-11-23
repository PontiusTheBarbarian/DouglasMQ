namespace DouglasMQ.Diagnostics
{
	/// <summary>
	/// Names of the <see cref="System.Diagnostics.ActivitySource"/> DouglasMQ emits traces through. Pass it
	/// to OpenTelemetry to collect DouglasMQ's publish and process spans.
	/// </summary>
	/// <example>
	/// <code>
	/// services.AddOpenTelemetry()
	///     .WithTracing(t => t.AddSource(DouglasMQInstrumentation.ActivitySourceName));
	/// </code>
	/// </example>
	public static class DouglasMQInstrumentation
	{
		/// <summary>
		/// The name of the activity source used for publish and process spans.
		/// </summary>
		public const string ActivitySourceName = "DouglasMQ";
	}
}
