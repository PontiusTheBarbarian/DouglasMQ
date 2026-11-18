# Releasing

## How releases work

CI (`.github/workflows/ci.yml` running the Cake build in `build/`) does everything. Nobody edits a version number by hand.

| What you do | What CI builds | Tests | Published to GitHub Packages |
|-------------|----------------|-------|------------------------------|
| Open or update a pull request | `X.Y.Z-PullRequestNNNN.N` | ✅ | ❌ |
| Push to a feature branch | `X.Y.Z-<branch>.N` | ✅ | ❌ |
| Merge to `main` | `X.Y.Z-ci.N` (prerelease, unique per merge) | ✅ | ✅ |
| Push a tag `vX.Y.Z` | `X.Y.Z` (stable) | ✅ | ✅ |

Versions come from [GitVersion](https://gitversion.net/) (`GitVersion.yml`). `X.Y.Z` is the last release tag with the minor version bumped. `N` counts commits since that tag. The same version is stamped on the assemblies (`AssemblyVersion`, `FileVersion`, `InformationalVersion`) and on the package.

### Cutting a stable release

1. Make sure `main` is green and `CHANGELOG.md` describes the release.
2. Tag the commit and push the tag:

   ```bash
   git tag v0.3.0
   git push origin v0.3.0
   ```

3. CI builds, tests and publishes `0.3.0`. The tag is used verbatim as the version, so `v1.0.0-rc.1` publishes `1.0.0-rc.1`.

A version can only be pushed to the feed once, so never re-tag a released version. Fix forward with the next version instead.

### Choosing the next version

DouglasMQ uses [Semantic Versioning](https://semver.org/):

| Change | Version bump | Example |
|--------|-------------|---------|
| Bug fix, no API change | Patch | `1.2.3` → `1.2.4` |
| New feature, backwards compatible | Minor | `1.2.3` → `1.3.0` |
| Breaking change | Major | `1.2.3` → `2.0.0` |

Before 1.0, breaking changes bump the minor version.

## Pre-release checklist (local)

CI runs all of this, but to check before tagging:

- [ ] `dotnet build -c Release`: zero warnings, zero errors
- [ ] `dotnet test`: unit, architecture and integration tests pass (integration tests need Docker)
- [ ] `dotnet pack src/DouglasMQ -c Release`: `.nupkg` and `.snupkg` produced
- [ ] Inspect the package contents (see below)

---

## Building the package

```bash
dotnet pack src/DouglasMQ -c Release -o ./artifacts
```

This produces two files in `./artifacts/`:
- `DouglasMQ.{version}.nupkg` — the package consumers install
- `DouglasMQ.{version}.snupkg` — symbol package for debugger step-through

### Inspecting package contents

Use the NuGet Package Explorer or extract the `.nupkg` (it is a zip):

```bash
# List contents
unzip -l artifacts/DouglasMQ.*.nupkg

# Verify these are present:
# lib/net10.0/DouglasMQ.dll         — the assembly
# lib/net10.0/DouglasMQ.xml         — XML docs (enables IntelliSense)
# README.md                         — shown on nuget.org package page
```

Key things to check:
- `DouglasMQ.xml` is present alongside the `.dll` (requires `<GenerateDocumentationFile>true</GenerateDocumentationFile>`)
- `README.md` is at the package root (requires the `<None Include="..." Pack="true" PackagePath="\"/>` item)
- No `.pdb` files inside the `.nupkg` — those should be in the separate `.snupkg`

---

## Source Link verification

Source Link lets NuGet consumers step through your library source in the debugger without cloning the repo. To verify it is working:

1. Pack with Release configuration (debug symbols are embedded in release builds via `<IncludeSymbols>true</IncludeSymbols>`).
2. In Visual Studio: **Tools → Options → Debugging → General** — enable *Enable Source Link support* and *Enable Source Server support*.
3. Add the published NuGet package to a test project, set a breakpoint, and step into a DouglasMQ method — VS should fetch and display the source file from GitHub.

To verify Source Link metadata in the package without Visual Studio:

```bash
dotnet tool install --global sourcelink
sourcelink test artifacts/DouglasMQ.*.snupkg
```

Expected output: `sourcelink test passed: N out of N files are trackable`

---

## Symbol packages (`.snupkg`)

The `.snupkg` file is uploaded to NuGet alongside the `.nupkg`. NuGet.org hosts symbols at `https://symbols.nuget.org` — Visual Studio queries this automatically.

The configuration that enables this:
```xml
<IncludeSymbols>true</IncludeSymbols>
<SymbolPackageFormat>snupkg</SymbolPackageFormat>
```

`snupkg` only contains `.pdb` files (not source). Source Link metadata inside the `.pdb` files points back to the GitHub commit, which is why Source Link and `.snupkg` work together.

---

## Deterministic builds

The project is configured for reproducible builds:

```xml
<Deterministic>true</Deterministic>
<ContinuousIntegrationBuild Condition="'$(CI)' == 'true'">true</ContinuousIntegrationBuild>
```

`Deterministic` ensures the same source code always produces byte-identical binaries. `ContinuousIntegrationBuild` (active only when the `CI` environment variable is set) locks file paths in the PDB to repo-relative paths so Source Link can resolve them from any machine.

Local builds are unaffected — `ContinuousIntegrationBuild` is only `true` in CI.

---

## Publishing to NuGet.org

### Manual push

```bash
dotnet nuget push artifacts/DouglasMQ.*.nupkg \
  --api-key $NUGET_API_KEY \
  --source https://api.nuget.org/v3/index.json \
  --skip-duplicate

dotnet nuget push artifacts/DouglasMQ.*.snupkg \
  --api-key $NUGET_API_KEY \
  --source https://api.nuget.org/v3/index.json \
  --skip-duplicate
```

Push the `.snupkg` separately — NuGet.org processes it independently and associates it with the matching package version.

### GitHub Actions CI/CD

Publishing is automated by the existing workflow (see [How releases work](#how-releases-work)). The `PublishTask` in `build/Build.cs` pushes only for `main` and `v*` tags.

## Private feed (Azure Artifacts / GitHub Packages)

To publish to a private feed instead of NuGet.org:

```bash
# Azure Artifacts
dotnet nuget add source https://pkgs.dev.azure.com/{org}/{project}/_packaging/{feed}/nuget/v3/index.json \
  --name AzureArtifacts \
  --username az \
  --password $AZURE_DEVOPS_TOKEN

dotnet nuget push artifacts/*.nupkg --source AzureArtifacts

# GitHub Packages
dotnet nuget push artifacts/*.nupkg \
  --source https://nuget.pkg.github.com/{owner}/index.json \
  --api-key $GITHUB_TOKEN
```
