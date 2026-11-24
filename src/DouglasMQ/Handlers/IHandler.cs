using System.Threading;
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
	/// <remarks>
	/// Implement exactly one of the two <c>HandleAsync</c> overloads; each one's default implementation
	/// calls the other. Implement <see cref="HandleAsync(TMessage, CancellationToken)"/> to be told when
	/// the host is shutting down; <see cref="HandleAsync(TMessage)"/> is kept so existing handlers
	/// compile unchanged. DouglasMQ always calls the overload that takes a token, and refuses at start-up
	/// a handler that implements neither.
	/// </remarks>
	public interface IHandler<in TMessage>
		where TMessage : class, IMessage
	{
		/// <summary>
		/// Handles a message of type <typeparamref name="TMessage"/>. Implement this overload, or
		/// <see cref="HandleAsync(TMessage, CancellationToken)"/> to receive the cancellation token.
		/// </summary>
		/// <param name="message">The message to be processed.</param>
		/// <exception cref="System.ArgumentNullException">Thrown when <paramref name="message"/> is <c>null</c>.</exception>
		Task HandleAsync(TMessage message) => HandleAsync(message, CancellationToken.None);

		/// <summary>
		/// Handles a message of type <typeparamref name="TMessage"/>.
		/// </summary>
		/// <param name="message">The message to be processed.</param>
		/// <param name="cancellationToken">
		/// Cancelled when the host is stopping and the handler is still running once the shutdown
		/// timeout has run out. Stopping waits for running handlers to finish first, so a handler that
		/// finishes in time is not cancelled. If the handler then throws
		/// <see cref="System.OperationCanceledException"/>, the delivery is neither acknowledged nor
		/// dead-lettered, and the broker redelivers it.
		/// </param>
		/// <exception cref="System.ArgumentNullException">Thrown when <paramref name="message"/> is <c>null</c>.</exception>
		Task HandleAsync(TMessage message, CancellationToken cancellationToken) => HandleAsync(message);
	}
}
