using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DouglasMQ
{
	/// <summary>
	/// Assembly scanning shared by <see cref="DouglasMQBuilder"/> and the Microsoft DI registration.
	/// </summary>
	internal static class HandlerDiscovery
	{
		/// <summary>
		/// Finds concrete, closed handler types in <paramref name="assembly"/>: classes decorated with
		/// <see cref="HandlerAttribute"/> that implement <see cref="IHandler{TMessage}"/>.
		/// </summary>
		/// <remarks>
		/// Abstract classes and open generics are skipped. <see cref="HandlerAttribute"/> is
		/// inherited, so a decorated abstract base would otherwise be picked up and fail to construct.
		/// </remarks>
		public static IEnumerable<Type> FindHandlerTypes(Assembly assembly)
		{
			ArgumentNullException.ThrowIfNull(assembly);

			return assembly
				.GetTypes()
				.Where(t => t is { IsClass: true, IsAbstract: false, ContainsGenericParameters: false })
				.Where(t => t.GetCustomAttribute<HandlerAttribute>() is not null)
				.Where(t => GetHandledMessageTypes(t).Count > 0);
		}

		/// <summary>
		/// Returns the <c>TMessage</c> of every <see cref="IHandler{TMessage}"/> that <paramref name="handlerType"/> implements.
		/// </summary>
		public static IReadOnlyList<Type> GetHandledMessageTypes(Type handlerType) =>
			handlerType.GetInterfaces()
				.Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandler<>))
				.Select(i => i.GetGenericArguments()[0])
				.ToList();
	}
}
