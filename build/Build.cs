using System;
using Build;
using Build.Extensions;
using Cake.Common;
using Cake.Common.Build;
using Cake.Common.Diagnostics;
using Cake.Common.IO;
using Cake.Common.Tools.DotNet;
using Cake.Common.Tools.DotNet.Build;
using Cake.Common.Tools.DotNet.Clean;
using Cake.Common.Tools.DotNet.Pack;
using Cake.Common.Tools.GitVersion;
using Cake.Core;
using Cake.Frosting;

/// <summary>
/// Versions the projects in the solution.
/// </summary>
[TaskName(nameof(VersionTask))]
public sealed class VersionTask : FrostingTask<BuildContext>
{
	/// <summary>
	/// <inheritdoc />
	/// </summary>
	/// <param name="context"><see cref="BuildContext"/>.</param>
	/// <returns>bool.</returns>
	public override bool ShouldRun(BuildContext context)
		=> context.IsRunningOnBuildServer;

	/// <summary>
	/// <inheritdoc />
	/// </summary>
	/// <param name="context"><see cref="BuildContext"/>.</param>
	public override void Run(BuildContext context)
	{
		context.CreateDirectory(context.ReportsDirectory);

		var versionInfo = context.GitVersion(new()
		{
			ConfigFile = BuildConstants.GitVersionFilePath,
			OutputType = context.IsRunningOnBuildServer
				? GitVersionOutput.BuildServer
				: GitVersionOutput.Json,
			Verbosity = GitVersionVerbosity.Verbose,
			UpdateProjectFiles = context.IsRunningOnBuildServer,
			Diag = true
		});

		context.WriteVersioningReportToOutputDirectory(versionInfo,
			CreateReportFilePath(context, BuildConstants.GitVersionJsonFile));
	}

	// Reports are uploaded by the 'Upload reports' step in the GitHub Actions workflow.

	private static string CreateReportFilePath(BuildContext context, string fileName)
	{
		const string template = "{0}/{1}";
		return string.Format(template, context.ReportsDirectory, fileName);
	}
}

[TaskName(nameof(CleanTask))]
[IsDependentOn(typeof(VersionTask))]
public sealed class CleanTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		context.Information($"Clean started.");
		context.DotNetClean(context.SolutionFile, new DotNetCleanSettings() 
		{
			Configuration = context.BuildConfiguration
		});

		if (!context.IsRunningOnBuildServer && context.DirectoryExists(context.ArtifactDirectory))
		{
			context.Information($"Cleaning {context.ArtifactDirectory}...");
			context.DeleteDirectory(context.ArtifactDirectory, new DeleteDirectorySettings()
			{
				Force = true,
				Recursive = true,
			});
		}

		// If we are running the project locally we want to create a local artifacts directory to output our projects
		if (!context.IsRunningOnBuildServer && !context.DirectoryExists("./artifacts"))
		{
			context.CreateDirectory("./artifacts");
		}
		context.Information($"Clean finished.");
	}
}

[TaskName(nameof(RestoreTask))]
[IsDependentOn(typeof(CleanTask))]
public sealed class RestoreTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		context.Information("Package restore starting.");
		context.DotNetRestore(context.SolutionFile);
		context.Information("Package restore finished.");
	}
}

[TaskName(nameof(BuildTask))]
[IsDependentOn(typeof(RestoreTask))]
public sealed class BuildTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		context.Information("Build starting.");
		context.DotNetBuild(context.SolutionFile, new DotNetBuildSettings
		{
			Configuration = context.BuildConfiguration
		});
		context.Information("Build finished.");
	}
}

[TaskName(nameof(PackTask))]
[IsDependentOn(typeof(BuildTask))]
public sealed class PackTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		context.Information("Pack starting.");
		const string version = "Version";
		context.DotNetPack(context.SolutionFile, new DotNetPackSettings
		{
			Configuration = context.BuildConfiguration,
			NoRestore = true,
			NoBuild = true,
			OutputDirectory = $"{context.ArtifactDirectory}/NuGet",
			ArgumentCustomization = args
				=> args.Append(CreateArgument(version, context.GitVersion().SemVer))
		});
		context.Information("Pack finished.");

		context.Information("NuGet push started.");
		const string glob = "*.nupkg";
		context.DotNetNuGetPush(CreatePathToPackages($"{context.ArtifactDirectory}/NuGet", glob), new()
		{
			Source = BuildConstants.NuGetFeedUrl,
			ApiKey = context.EnvironmentVariable(BuildConstants.NUGET_ACCESS_TOKEN)
		});
		context.Information("NuGet push finished.");
	}

	private static string CreateArgument(string name, string value)
	{
		const string template = "/p:{0}={1}";
		return string.Format(template, name, value);
	}

	private static string CreatePathToPackages(string directory, string glob)
	{
		const string template = "{0}/{1}";
		return string.Format(template, directory, glob);
	}
}

[TaskName(nameof(UploadArtifactsTask))]
[IsDependentOn(typeof(PackTask))]
public sealed class UploadArtifactsTask : FrostingTask<BuildContext>
{
	public override bool ShouldRun(BuildContext context) 
		=> context.IsRunningOnBuildServer;

	public override void Run(BuildContext context)
	{
		// Artifact upload is handled by the 'Upload NuGet packages' step in the GitHub Actions workflow.
	}
}

[IsDependentOn(typeof(UploadArtifactsTask))]
[TaskName(nameof(Default))]
public sealed class Default : FrostingTask // Entry point
{
}