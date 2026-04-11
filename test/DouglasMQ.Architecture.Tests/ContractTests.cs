using System;
using System.Linq;
using System.Reflection;
using DouglasMQ.Messages;
using DouglasMQ.Serialization;
using NetArchTest.Rules;
using Xunit;

namespace DouglasMQ.Architecture.Tests
{
	/// <summary>
	/// Verifies that the library's own contracts are self-consistent.
	/// These rules prevent regressions where a type wears an attribute but fails to implement
	/// the matching interface, which would cause a silent runtime failure.
	/// </summary>
	public class ContractTests
	{
		private static readonly Assembly LibraryAssembly = typeof(Publisher).Assembly;

		[Fact]
		public void Publisher_ImplementsIPublisher()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Publisher")
				.And().AreClasses()
				.Should().ImplementInterface(typeof(IPublisher))
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"All Publisher classes must implement IPublisher. Failing: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void TypesWithHandlerAttribute_ImplementIHandler()
		{
			// Any class decorated with [HandlerAttribute] must implement IHandler<T>.
			// If this contract breaks, the Consumer will throw at runtime when resolving the handler.
			var violations = LibraryAssembly
				.GetTypes()
				.Where(t => t.GetCustomAttribute<HandlerAttribute>() is not null)
				.Where(t => !t.GetInterfaces().Any(i =>
					i.IsGenericType &&
					i.GetGenericTypeDefinition() == typeof(IHandler<>)))
				.Select(t => t.FullName)
				.ToList();

			Assert.Empty(violations);
		}

		[Fact]
		public void TypesWithMessageAttribute_ImplementIMessage()
		{
			// Any class decorated with [MessageAttribute] must implement IMessage.
			// If this contract breaks, the Publisher will throw at runtime when resolving routing info.
			var violations = LibraryAssembly
				.GetTypes()
				.Where(t => t.GetCustomAttribute<MessageAttribute>() is not null)
				.Where(t => !typeof(IMessage).IsAssignableFrom(t))
				.Select(t => t.FullName)
				.ToList();

			Assert.Empty(violations);
		}

		[Fact]
		public void ISerializer_HasBothSerializeAndDeserialize()
		{
			// Ensures the serialization contract stays symmetrical.
			var serializerInterface = typeof(ISerializer);
			Assert.NotNull(serializerInterface.GetMethod("Serialize"));
			Assert.NotNull(serializerInterface.GetMethod("Deserialize"));
		}
	}
}
