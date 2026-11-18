using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using DouglasMQ.Messages;

namespace DouglasMQ
{
	/// <summary>
	/// A strongly-typed call to <see cref="IHandler{TMessage}.HandleAsync"/>, built once per handler
	/// at startup so no reflection happens per message.
	/// </summary>
	/// <remarks>
	/// Dispatching through the <see cref="IHandler{TMessage}"/> interface (rather than looking up a
	/// public method named <c>HandleAsync</c>) means explicit interface implementations work, and a
	/// handler implementing several <see cref="IHandler{TMessage}"/> interfaces is not ambiguous.
	/// The message type no longer needs a <see cref="MessageAttribute"/> on the consuming side.
	/// </remarks>
	internal sealed class HandlerInvoker
	{
		private readonly Func<object, Task> _invoke;

		private HandlerInvoker(Type messageType, string handlerName, Func<object, Task> invoke)
		{
			MessageType = messageType;
			HandlerName = handlerName;
			_invoke = invoke;
		}

		public Type MessageType { get; }

		public string HandlerName { get; }

		public Task InvokeAsync(object message) => _invoke(message);

		/// <summary>
		/// Builds an invoker for <paramref name="handler"/>.
		/// </summary>
		/// <param name="handler">An object implementing one or more <see cref="IHandler{TMessage}"/>.</param>
		/// <param name="messageType">
		/// Which <see cref="IHandler{TMessage}"/> to dispatch to. When <c>null</c>, the handler must
		/// implement exactly one.
		/// </param>
		/// <exception cref="InvalidOperationException">The message type cannot be determined.</exception>
		public static HandlerInvoker Create(object handler, Type? messageType = null)
		{
			ArgumentNullException.ThrowIfNull(handler);

			var handlerType = handler.GetType();
			var handledTypes = handlerType.GetInterfaces()
				.Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandler<>))
				.Select(i => i.GetGenericArguments()[0])
				.ToArray();

			if (messageType is null)
			{
				if (handledTypes.Length != 1)
				{
					throw new InvalidOperationException(
						$"Handler '{handlerType.Name}' must implement exactly one {nameof(IHandler<IMessage>)}<T> " +
						$"(found {handledTypes.Length}). Register it once per message type instead.");
				}

				messageType = handledTypes[0];
			}
			else if (!handledTypes.Contains(messageType))
			{
				throw new InvalidOperationException(
					$"Handler '{handlerType.Name}' does not implement {nameof(IHandler<IMessage>)}<{messageType.Name}>.");
			}

			var factory = typeof(HandlerInvoker)
				.GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static)!
				.MakeGenericMethod(messageType);

			return (HandlerInvoker)factory.Invoke(null, new[] { handler })!;
		}

		private static HandlerInvoker CreateTyped<TMessage>(object handler)
			where TMessage : class, IMessage
		{
			var typed = (IHandler<TMessage>)handler;
			return new HandlerInvoker(typeof(TMessage), handler.GetType().Name, message => typed.HandleAsync((TMessage)message));
		}
	}
}
