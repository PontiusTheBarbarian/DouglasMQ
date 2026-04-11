using System.IO;
using Cake.Common.Tools.GitVersion;
using Cake.Core.Diagnostics;
using NuGet.Protocol;

namespace Build.Extensions
{
	internal static class BuildContextExtensions
	{
		/// <summary>
		/// Writes the versioning report to a json file.
		/// </summary>
		/// <param name="context">Context.</param>
		/// <param name="versionInfo">Version info.</param>
		/// <param name="file">File.</param>
		internal static void WriteVersioningReportToOutputDirectory(this BuildContext context, GitVersion versionInfo, string file)
		{
			var json = versionInfo.ToJson(Newtonsoft.Json.Formatting.Indented);
			File.WriteAllText(file, json);
			context.Log.Information(versionInfo.ToJson(Newtonsoft.Json.Formatting.Indented));
		}
	}
}
