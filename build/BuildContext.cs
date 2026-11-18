#nullable enable
using System;
using Cake.Common;
using Cake.Common.Build;
using Cake.Core;
using Cake.Frosting;

public class BuildContext : FrostingContext
{
	private const string MainBranchRef = "refs/heads/main";
	private const string ReleaseTagRefPrefix = "refs/tags/v";

	/// <summary>
	/// The solution file to target
	/// </summary>
	public string SolutionFile { get; }

	/// <summary>
	/// The output directory where build artifacts will be placed
	/// </summary>
	public string ArtifactDirectory { get; }

	/// <summary>
	/// The directory where reports will be placed
	/// </summary>
	public string ReportsDirectory { get; }

	/// <summary>
	/// The build configuration
	/// </summary>
	public string BuildConfiguration { get; }

	/// <summary>
	/// Is the project running on a build server e.g. GitHub Actions
	/// </summary>
	public bool IsRunningOnBuildServer { get; }

	/// <summary>
	/// Is the build a pull request build
	/// </summary>
	public bool IsPullRequestBuild { get; }

	/// <summary>
	/// The git ref being built, e.g. <c>refs/heads/main</c> or <c>refs/tags/v1.2.0</c>. Empty locally.
	/// </summary>
	public string GitRef { get; }

	/// <summary>
	/// The release version when building a <c>v*</c> tag (e.g. <c>1.2.0</c> for <c>refs/tags/v1.2.0</c>); otherwise null.
	/// </summary>
	public string? ReleaseTagVersion { get; }

	/// <summary>
	/// Whether packages should be pushed to the feed. Only pushes to <c>main</c> (prerelease
	/// <c>X.Y.Z-ci.N</c> builds) and <c>v*</c> tags (stable releases) publish; pull requests and
	/// feature branches build and test only.
	/// </summary>
	public bool ShouldPublish =>
		IsRunningOnBuildServer &&
		!IsPullRequestBuild &&
		(GitRef == MainBranchRef || ReleaseTagVersion is not null);

	/// <summary>
	/// Versions computed by <c>VersionTask</c>. Null for local builds, which use the SDK defaults.
	/// </summary>
	public BuildVersion? Version { get; set; }

	public BuildContext(ICakeContext context)
		: base(context)
	{
		SolutionFile = context.EnvironmentVariable("SOLUTION_FILE", "../DouglasMQ.sln");
		ArtifactDirectory = context.EnvironmentVariable("ARTIFACT_DIRECTORY", "./artifacts");
		ReportsDirectory = context.EnvironmentVariable("REPORTS_DIRECTORY", $"{ArtifactDirectory}/reports");
		BuildConfiguration = context.EnvironmentVariable("BUILD_CONFIGURATION", "Release");

		IsRunningOnBuildServer = context.BuildSystem().IsRunningOnGitHubActions;

		// Read GitHub's own variables rather than the typed Cake wrapper: they are simply absent
		// for local builds instead of throwing on a null event name.
		IsPullRequestBuild = string.Equals(context.EnvironmentVariable("GITHUB_EVENT_NAME"), "pull_request", StringComparison.OrdinalIgnoreCase);
		GitRef = context.EnvironmentVariable("GITHUB_REF") ?? string.Empty;
		ReleaseTagVersion = GitRef.StartsWith(ReleaseTagRefPrefix, StringComparison.Ordinal)
			? GitRef[ReleaseTagRefPrefix.Length..]
			: null;
	}
}

/// <summary>
/// The version numbers stamped onto assemblies and packages.
/// </summary>
/// <param name="PackageVersion">NuGet package version, e.g. <c>0.2.0-ci.4</c> or <c>0.2.0</c>.</param>
/// <param name="AssemblyVersion">Assembly version, e.g. <c>0.2.0.0</c>.</param>
/// <param name="InformationalVersion">Informational version including the commit, e.g. <c>0.2.0-ci.4+Sha.abc123</c>.</param>
public sealed record BuildVersion(string PackageVersion, string AssemblyVersion, string InformationalVersion)
{
	/// <summary>MSBuild arguments that apply this version to build and pack.</summary>
	public string ToMSBuildArguments() =>
		$"/p:Version={PackageVersion} /p:AssemblyVersion={AssemblyVersion} /p:FileVersion={AssemblyVersion} /p:InformationalVersion={InformationalVersion}";
}
