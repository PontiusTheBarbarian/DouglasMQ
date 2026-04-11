using System;
using Cake.Frosting;

namespace Build.Extensions
{
	internal static class CakeHostExtensions
	{
		internal static CakeHost UseTools(this CakeHost host)
			=> host
			.SetToolPath($"{Environment.CurrentDirectory}/tools")
			.InstallTool(CreatePackageUri("nuget", "NuGet.CommandLine", "6.11.1"))
			.InstallTool(CreatePackageUri("dotnet", "GitVersion.Tool", "5.12.0"))
			.InstallTool(CreatePackageUri("dotnet", "dotnet-reportgenerator-globaltool", "5.3.10"));

		private static Uri CreatePackageUri(string source, string name, string version)
		{
			const string template = "{0}:?package={1}&version={2}";
			return new Uri(string.Format(template, source, name, version));
		}
	}
}
