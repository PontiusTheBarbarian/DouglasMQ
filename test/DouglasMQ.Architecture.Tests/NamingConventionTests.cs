using System.Reflection;
using NetArchTest.Rules;
using Xunit;

namespace DouglasMQ.Architecture.Tests
{
	/// <summary>
	/// Verifies naming conventions and structural patterns in the public API.
	/// Consistent naming makes the library discoverable and predictable for consumers.
	/// </summary>
	public class NamingConventionTests
	{
		private static readonly Assembly LibraryAssembly = typeof(Publisher).Assembly;

		[Fact]
		public void Interfaces_HaveIPrefix()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().AreInterfaces()
				.Should().HaveNameStartingWith("I")
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"All interfaces must be prefixed with 'I'. Failing: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void Publisher_IsSealed()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().HaveNameEndingWith("Publisher")
				.And().AreClasses()
				.Should().BeSealed()
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"Publisher classes must be sealed: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}

		[Fact]
		public void PublicTypes_ResideInDouglasMQNamespace()
		{
			var result = Types.InAssembly(LibraryAssembly)
				.That().ArePublic()
				.Should().ResideInNamespaceStartingWith("DouglasMQ")
				.GetResult();

			Assert.True(result.IsSuccessful,
				$"All public types must be in a DouglasMQ namespace. Failing: {string.Join(", ", result.FailingTypeNames ?? [])}");
		}
	}
}
