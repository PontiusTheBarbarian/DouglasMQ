using System.Threading.Tasks;

namespace DouglasMQ
{
	/// <summary>
	/// Defines the contract for a message publisher that can publish events and send commands to a message broker.
	/// </summary>
	public interface IPublisher
	{
		/// <summary>
		/// Publishes an event to the message broker.
		/// </summary>
		/// <typeparam name="TEvent">The type of the event to be published. Must implement <see cref="IEvent"/>.</typeparam>
		/// <param name="event">The event to be published.</param>
		/// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
		/// <exception cref="System.ArgumentNullException">Thrown when <paramref name="event"/> is <c>null</c>.</exception>
		ValueTask PublishAsync<TEvent>(TEvent @event)
			where TEvent : class, IEvent;

		/// <summary>
		/// Sends a command to the message broker.
		/// </summary>
		/// <typeparam name="TCommand">The type of the command to be sent. Must implement <see cref="ICommand"/>.</typeparam>
		/// <param name="command">The command to be sent.</param>
		/// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
		/// <exception cref="System.ArgumentNullException">Thrown when <paramref name="command"/> is <c>null</c>.</exception>
		ValueTask SendAsync<TCommand>(TCommand command)
			where TCommand : class, ICommand;
	}
}
