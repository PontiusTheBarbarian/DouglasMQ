namespace DouglasMQ.Sample;

public sealed class SampleOptions
{
	/// <summary>
	/// Seconds between automatically generated orders. 0 disables the generator.
	/// </summary>
	public double AutoPublishIntervalSeconds { get; set; } = 5;

	/// <summary>
	/// Probability (0-1) that <c>OrderPlacedHandler</c> throws a transient error, which triggers a retry.
	/// </summary>
	public double FailureRate { get; set; } = 0.25;

	/// <summary>
	/// Probability (0-1) that a generated order is poison and ends up dead-lettered.
	/// </summary>
	public double PoisonRate { get; set; } = 0.05;
}
