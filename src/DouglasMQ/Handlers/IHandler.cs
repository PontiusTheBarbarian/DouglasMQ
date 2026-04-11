using System.Threading.Tasks;
using DouglasMQ.Messages;

namespace DouglasMQ
{
	/// <summary>
	/// Defines the contract for a handler that processes messages of a specific type.
	/// </summary>
	/// <typeparam name="TMessage">
	/// The type of message to be handled. Must implement <see cref="IMessage"/>.
	/// </typeparam>
	public interface IHandler<in TMessage>
		where TMessage : class, IMessage
	{
		/// <summary>
		/// Handles a message of type <typeparamref name="TMessage"/>.
		/// </summary>
		/// <param name="message">The message to be processed.</param>
		/// <exception cref="System.ArgumentNullException">Thrown when <paramref name="message"/> is <c>null</c>.</exception>
		Task HandleAsync(TMessage message);
	}
}
