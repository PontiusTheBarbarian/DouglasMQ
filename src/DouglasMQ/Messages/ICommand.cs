using DouglasMQ.Messages;

namespace DouglasMQ
{
	/// <summary>
	/// Represents a command message that is sent to a message broker for processing.
	/// Commands are typically used to request actions or operations to be performed.
	/// </summary>
	public interface ICommand : IMessage
	{
	}
}
