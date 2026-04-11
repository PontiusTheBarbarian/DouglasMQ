using System;

namespace DouglasMQ.Messages
{
	/// <summary>
	/// Base contract for all messages exchanged via DouglasMQ.
	/// Every message carries a unique identifier and a trace identifier for distributed tracing.
	/// </summary>
	public interface IMessage
	{
		/// <summary>
		/// Gets the unique identifier of this message instance.
		/// Implementations should return a stable value (not <c>Guid.NewGuid()</c> on each access).
		/// </summary>
		Guid MessageId { get; }

		/// <summary>
		/// Gets the trace identifier used to correlate related messages across service boundaries.
		/// Implementations should propagate this value from an incoming request or generate it once.
		/// </summary>
		Guid TraceId { get; }
	}
}
