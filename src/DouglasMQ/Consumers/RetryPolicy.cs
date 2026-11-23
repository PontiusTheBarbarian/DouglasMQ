using System;

namespace DouglasMQ
{
	/// <summary>
	/// How often, and how long apart, a failing handler is retried before its message is dead-lettered.
	/// </summary>
	internal sealed class RetryPolicy
	{
		/// <summary>Upper bound on a single retry delay, however large the retry count.</summary>
		public static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);

		private readonly int _baseDelayMs;

		/// <param name="retryCount">Retries after the first attempt. Negative values are treated as 0.</param>
		/// <param name="baseDelayMs">Delay before the first retry. Negative values are treated as 0.</param>
		public RetryPolicy(int retryCount, int baseDelayMs)
		{
			RetryCount = Math.Max(0, retryCount);
			_baseDelayMs = Math.Max(0, baseDelayMs);
		}

		/// <summary>Retries after the first attempt.</summary>
		public int RetryCount { get; }

		/// <summary>Total attempts: the first one plus <see cref="RetryCount"/> retries.</summary>
		public int MaxAttempts => RetryCount + 1;

		/// <summary>Reads <see cref="Configuration.RetryCount"/> and <see cref="Configuration.RetryDelayMs"/>.</summary>
		public static RetryPolicy From(Configuration configuration) => new(configuration.RetryCount, configuration.RetryDelayMs);

		/// <summary>
		/// Exponential backoff: base delay × 2^(retry-1), capped at <see cref="MaxDelay"/> so large retry
		/// counts cannot overflow or produce an invalid <see cref="System.Threading.Tasks.Task.Delay(TimeSpan)"/> argument.
		/// </summary>
		/// <param name="retry">The retry number, starting at 1.</param>
		public TimeSpan GetDelay(int retry)
		{
			var delayMs = _baseDelayMs * Math.Pow(2, retry - 1);
			return TimeSpan.FromMilliseconds(Math.Min(delayMs, MaxDelay.TotalMilliseconds));
		}
	}
}
