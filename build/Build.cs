#nullable enable
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
using Cake.Common.Tools.DotNet.Test;
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
			// Json, not BuildServer: with BuildServer output Cake returns an empty result object.
			OutputType = GitVersionOutput.Json,
			Verbosity = GitVersionVerbosity.Verbose,
			// Versions are passed to build/pack as MSBuild properties instead (see BuildVersion),
			// so project files are never rewritten and one value flows to both DLL and package.
			UpdateProjectFiles = false,
			Diag = true
		});

		context.WriteVersioningReportToOutputDirectory(versionInfo,
			CreateReportFilePath(context, BuildConstants.GitVersionJsonFile));

		if (string.IsNullOrWhiteSpace(versionInfo.SemVer))
		{
			throw new InvalidOperationException("GitVersion did not return a version.");
		}

		context.Version = context.ReleaseTagVersion is { } tagVersion
			// Tag build: the tag is the release version, exactly. GitVersion would apply the
			// branch label because the tag ref does not match the 'main' branch config.
			? new BuildVersion(
				tagVersion,
				$"{tagVersion.Split('-', '+')[0]}.0",
				$"{tagVersion}+Sha.{versionInfo.Sha}")
			: new BuildVersion(versionInfo.SemVer, versionInfo.AssemblySemVer, versionInfo.InformationalVersion);

		context.Information($"Version: {context.Version.PackageVersion} (ref '{context.GitRef}', publish: {context.ShouldPublish}).");
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
			Configuration = context.BuildConfiguration,
			NoRestore = true,
			// Stamp the GitVersion version on the assemblies themselves. Pack runs with --no-build,
			// so passing the version only at pack time would leave the DLLs at the SDK default.
			ArgumentCustomization = args => context.Version is null ? args : args.Append(context.Version.ToMSBuildArguments())
		});
		context.Information("Build finished.");
	}
}

[TaskName(nameof(TestTask))]
[IsDependentOn(typeof(BuildTask))]
public sealed class TestTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		// Runs unit, architecture and integration tests. Integration tests start RabbitMQ with
		// Testcontainers, so the runner needs Docker (GitHub's ubuntu runners have it).
		context.Information("Tests starting.");
		context.DotNetTest(context.SolutionFile, new DotNetTestSettings
		{
			Configuration = context.BuildConfiguration,
			NoRestore = true,
			NoBuild = true,
			ResultsDirectory = $"{context.ReportsDirectory}/tests",
			Loggers = new[] { "trx" },
		});
		context.Information("Tests finished.");
	}
}

[TaskName(nameof(PackTask))]
[IsDependentOn(typeof(TestTask))]
public sealed class PackTask : FrostingTask<BuildContext>
{
	public override void Run(BuildContext context)
	{
		context.Information("Pack starting.");
		context.DotNetPack(context.SolutionFile, new DotNetPackSettings
		{
			Configuration = context.BuildConfiguration,
			NoRestore = true,
			NoBuild = true,
			OutputDirectory = $"{context.ArtifactDirectory}/NuGet",
			ArgumentCustomization = args => context.Version is null ? args : args.Append(context.Version.ToMSBuildArguments())
		});
		context.Information("Pack finished.");
	}
}

[TaskName(nameof(PublishTask))]
[IsDependentOn(typeof(PackTask))]
public sealed class PublishTask : FrostingTask<BuildContext>
{
	public override bool ShouldRun(BuildContext context)
	{
		if (!context.ShouldPublish)
		{
			context.Information($"Skipping NuGet push for ref '{context.GitRef}': only main and v* tags publish.");
		}

		return context.ShouldPublish;
	}

	public override void Run(BuildContext context)
	{
		context.Information($"NuGet push of {context.Version?.PackageVersion} started.");
		context.DotNetNuGetPush($"{context.ArtifactDirectory}/NuGet/*.nupkg", new()
		{
			Source = BuildConstants.NuGetFeedUrl,
			ApiKey = context.EnvironmentVariable(BuildConstants.NUGET_ACCESS_TOKEN)
		});
		context.Information("NuGet push finished.");
	}
}

[TaskName(nameof(UploadArtifactsTask))]
[IsDependentOn(typeof(PublishTask))]
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