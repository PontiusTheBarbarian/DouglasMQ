using System.Reflection;
using NetArchTest.Rules;
using Xunit;

namespace DouglasMQ.Architecture.Tests
{
	/// <summary>
	/// Verifies that implementation details remain hidden behind internal access modifiers.
	/// Internal types should never leak into the public API surface.
	/// </summary>
	public class EncapsulationTests
	{
		private static readonly Assembly LibraryAssembly = typeof(Publisher).Assembly;

		[Fact]
		public void Consumer_IsNotPublic()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Consumer")
				.Should().NotBePublic()
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"Expected Consumer to be internal but found public types: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void Dispatcher_IsNotPublic()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Dispatcher")
				.Should().NotBePublic()
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"Expected Dispatcher to be internal but found public types: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void Consumer_IsSealed()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Consumer")
				.Should().BeSealed()
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"Expected Consumer to be sealed: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void Dispatcher_IsSealed()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Dispatcher")
				.Should().BeSealed()
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"Expected Dispatcher to be sealed: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}
	}
}
