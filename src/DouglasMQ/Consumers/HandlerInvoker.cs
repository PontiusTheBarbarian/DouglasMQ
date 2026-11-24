using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Messages;
using Microsoft.Extensions.DependencyInjection;

namespace DouglasMQ
{
	/// <summary>
	/// A strongly-typed call to <see cref="IHandler{TMessage}.HandleAsync(TMessage, CancellationToken)"/>, built once per handler
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
		private readonly Func<object, CancellationToken, Task> _invoke;

		private HandlerInvoker(Type messageType, Type handlerType, Func<object, CancellationToken, Task> invoke)
		{
			MessageType = messageType;
			HandlerType = handlerType;
			_invoke = invoke;
		}

		public Type MessageType { get; }

		/// <summary>The handler's concrete type.</summary>
		public Type HandlerType { get; }

		public string HandlerName => HandlerType.Name;

		/// <summary>Calls the handler.</summary>
		/// <param name="message">The deserialized message.</param>
		/// <param name="cancellationToken">Passed to <see cref="IHandler{TMessage}.HandleAsync(TMessage, CancellationToken)"/>.</param>
		public Task InvokeAsync(object message, CancellationToken cancellationToken = default) => _invoke(message, cancellationToken);

		/// <summary>
		/// Builds an invoker that sends every message to the same <paramref name="handler"/> instance.
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
			messageType = ResolveMessageType(handlerType, messageType);
			var dispatch = CreateDispatch(messageType);

			return new HandlerInvoker(messageType, handlerType, (message, cancellationToken) => dispatch(handler, message, cancellationToken));
		}

		/// <summary>
		/// Builds an invoker that creates a new dependency injection scope for every call, activates
		/// the handler inside it, and disposes the handler and the scope once the call completes.
		/// </summary>
		/// <param name="scopeFactory">Creates the per-call scope.</param>
		/// <param name="handlerType">The handler's concrete type, used for validation and logging.</param>
		/// <param name="messageType">Which <see cref="IHandler{TMessage}"/> to dispatch to.</param>
		/// <param name="activate">
		/// Creates the handler from the scope's provider. The invoker owns the result and disposes it,
		/// because handlers activated outside the container are not tracked by the scope.
		/// </param>
		/// <exception cref="InvalidOperationException"><paramref name="handlerType"/> does not handle <paramref name="messageType"/>.</exception>
		public static HandlerInvoker CreateScoped(
			IServiceScopeFactory scopeFactory,
			Type handlerType,
			Type messageType,
			Func<IServiceProvider, object> activate)
		{
			ArgumentNullException.ThrowIfNull(scopeFactory);
			ArgumentNullException.ThrowIfNull(handlerType);
			ArgumentNullException.ThrowIfNull(activate);

			messageType = ResolveMessageType(handlerType, messageType);
			var dispatch = CreateDispatch(messageType);

			return new HandlerInvoker(messageType, handlerType, async (message, cancellationToken) =>
			{
				await using var scope = scopeFactory.CreateAsyncScope();
				var handler = activate(scope.ServiceProvider);
				try
				{
					await dispatch(handler, message, cancellationToken);
				}
				finally
				{
					await DisposeHandlerAsync(handler);
				}
			});
		}

		private static Type ResolveMessageType(Type handlerType, Type? messageType)
		{
			var handledTypes = HandlerDiscovery.GetHandledMessageTypes(handlerType);

			if (messageType is null)
			{
				if (handledTypes.Count != 1)
				{
					throw new InvalidOperationException(
						$"Handler '{handlerType.Name}' must implement exactly one {nameof(IHandler<IMessage>)}<T> " +
						$"(found {handledTypes.Count}). Register it once per message type instead.");
				}

				return EnsureImplementsHandleAsync(handlerType, handledTypes[0]);
			}

			if (!handledTypes.Contains(messageType))
			{
				throw new InvalidOperationException(
					$"Handler '{handlerType.Name}' does not implement {nameof(IHandler<IMessage>)}<{messageType.Name}>.");
			}

			return EnsureImplementsHandleAsync(handlerType, messageType);
		}

		/// <summary>
		/// Both <c>HandleAsync</c> overloads have default implementations that call each other, so a
		/// handler that implements neither would recurse until the stack overflows on its first message.
		/// </summary>
		private static Type EnsureImplementsHandleAsync(Type handlerType, Type messageType)
		{
			if (handlerType.IsInterface)
			{
				return messageType;
			}

			var interfaceType = typeof(IHandler<>).MakeGenericType(messageType);
			var map = handlerType.GetInterfaceMap(interfaceType);
			if (map.TargetMethods.All(m => m.DeclaringType == interfaceType))
			{
				throw new InvalidOperationException(
					$"Handler '{handlerType.Name}' implements {nameof(IHandler<IMessage>)}<{messageType.Name}> but neither HandleAsync overload. " +
					"Implement HandleAsync(message, cancellationToken) (or HandleAsync(message)).");
			}

			return messageType;
		}

		/// <summary>
		/// Disposes a handler that was activated outside the container, which the scope does not track.
		/// </summary>
		internal static async ValueTask DisposeHandlerAsync(object handler)
		{
			switch (handler)
			{
				case IAsyncDisposable asyncDisposable:
					await asyncDisposable.DisposeAsync();
					break;
				case IDisposable disposable:
					disposable.Dispose();
					break;
			}
		}

		private static Func<object, object, CancellationToken, Task> CreateDispatch(Type messageType)
		{
			var factory = typeof(HandlerInvoker)
				.GetMethod(nameof(CreateTypedDispatch), BindingFlags.NonPublic | BindingFlags.Static)!
				.MakeGenericMethod(messageType);

			return (Func<object, object, CancellationToken, Task>)factory.Invoke(null, null)!;
		}

		private static Func<object, object, CancellationToken, Task> CreateTypedDispatch<TMessage>()
			where TMessage : class, IMessage
			=> (handler, message, cancellationToken) => ((IHandler<TMessage>)handler).HandleAsync((TMessage)message, cancellationToken);
	}
}
