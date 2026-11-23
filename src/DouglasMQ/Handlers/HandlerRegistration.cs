using System;
using System.Reflection;

namespace DouglasMQ
{
	/// <summary>
	/// One consumer to start: the queue it reads (<see cref="Binding"/>) and how its messages reach
	/// the handler (<see cref="Invoker"/>). This is the only thing the DI registration and
	/// <see cref="DouglasMQBuilder"/> each produce; everything after it is shared.
	/// </summary>
	internal sealed class HandlerRegistration
	{
		private HandlerRegistration(HandlerAttribute binding, HandlerInvoker invoker)
		{
			Binding = binding;
			Invoker = invoker;
		}

		/// <summary>The handler's exchange, queue and routing key.</summary>
		public HandlerAttribute Binding { get; }

		/// <summary>Dispatches deserialized messages to the handler.</summary>
		public HandlerInvoker Invoker { get; }

		/// <summary>
		/// Pairs <paramref name="invoker"/> with the <see cref="HandlerAttribute"/> on its handler type.
		/// </summary>
		/// <exception cref="InvalidOperationException">The handler type is not decorated with <see cref="HandlerAttribute"/>.</exception>
		public static HandlerRegistration For(HandlerInvoker invoker)
		{
			ArgumentNullException.ThrowIfNull(invoker);

			var binding = invoker.HandlerType.GetCustomAttribute<HandlerAttribute>()
				?? throw new InvalidOperationException(
					$"Handler '{invoker.HandlerName}' must be decorated with [{nameof(HandlerAttribute)}] " +
					"so DouglasMQ knows which queue it consumes.");

			return new HandlerRegistration(binding, invoker);
		}
	}
}
