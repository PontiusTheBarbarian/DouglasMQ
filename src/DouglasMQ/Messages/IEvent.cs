using DouglasMQ.Messages;

namespace DouglasMQ
{
	/// <summary>
	/// Represents an event message that is published to a message broker to notify subscribers about a specific occurrence.
	/// Events are typically used for broadcasting state changes or system activities.
	/// </summary>
	public interface IEvent : IMessage
	{
	}
}
