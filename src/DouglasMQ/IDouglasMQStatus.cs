namespace DouglasMQ
{
	/// <summary>
	/// Reports whether DouglasMQ's consumers are running. Registered by <c>AddDouglasMQ</c>, where
	/// consumers start in the background and keep retrying while the broker is unreachable, so an
	/// application can surface that state (for example in a readiness check).
	/// </summary>
	public interface IDouglasMQStatus
	{
		/// <summary>
		/// <c>true</c> once the consumer connection is open, the dead-letter topology is declared and every
		/// consumer has started; <c>false</c> before that and after stopping. After start-up, the connection's
		/// automatic recovery handles broker restarts and this stays <c>true</c>.
		/// </summary>
		bool ConsumersRunning { get; }
	}
}
