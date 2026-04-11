using System;
using Cake.Common;
using Cake.Common.Build;
using Cake.Core;
using Cake.Frosting;

public class BuildContext : FrostingContext
{
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

	public BuildContext(ICakeContext context)
		: base(context)
	{
		SolutionFile = context.EnvironmentVariable("SOLUTION_FILE", "../DouglasMQ.sln");
		ArtifactDirectory = context.EnvironmentVariable("ARTIFACT_DIRECTORY", "./artifacts");
		ReportsDirectory = context.EnvironmentVariable("REPORTS_DIRECTORY", $"{ArtifactDirectory}/reports");
		BuildConfiguration = context.EnvironmentVariable("BUILD_CONFIGURATION", "Release");

		IsRunningOnBuildServer = context.BuildSystem().IsRunningOnGitHubActions;
		IsPullRequestBuild = context.GitHubActions().Environment.Workflow.EventName
			.Equals("pull_request", StringComparison.OrdinalIgnoreCase);
	}
}

