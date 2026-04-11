# Releasing to NuGet

## Pre-release checklist

Before publishing a new version, verify:

- [ ] `<Version>` in `src/DouglasMQ/DouglasMQ.csproj` is bumped (see versioning below)
- [ ] `CHANGELOG.md` entry written for this version
- [ ] `dotnet build -c Release` — zero warnings, zero errors
- [ ] `dotnet test test/DouglasMQ.Unit.Tests` — all unit tests pass (no Docker required)
- [ ] `dotnet test test/DouglasMQ.Architecture.Tests` — all architecture rules pass
- [ ] `dotnet test test/DouglasMQ.Integration.Tests` — all integration tests pass (Docker required)
- [ ] `dotnet pack src/DouglasMQ -c Release` — `.nupkg` and `.snupkg` produced without errors
- [ ] Inspect the package contents (see below)

---

## Versioning

DouglasMQ uses [Semantic Versioning](https://semver.org/):

| Change | Version bump | Example |
|--------|-------------|---------|
| Bug fix, no API change | Patch | `1.2.3` → `1.2.4` |
| New feature, backwards compatible | Minor | `1.2.3` → `1.3.0` |
| Breaking change | Major | `1.2.3` → `2.0.0` |

Set the version in `src/DouglasMQ/DouglasMQ.csproj`:

```xml
<Version>1.0.0</Version>
<AssemblyVersion>1.0.0</AssemblyVersion>
<FileVersion>1.0.0</FileVersion>
```

### GitVersion (optional)

For automatic version management from git history and tags, add [GitVersion](https://gitversion.net/):

```bash
dotnet tool install --global GitVersion.Tool
```

`GitVersion.yml` at the repo root:
```yaml
mode: ContinuousDelivery
branches:
  main:
    tag: ''
    increment: Patch
  feature:
    tag: alpha
    increment: Minor
```

Then in CI:
```yaml
- name: Set version
  run: dotnet-gitversion /updateprojectfiles
```

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

```yaml
name: Publish NuGet

on:
  push:
    tags:
      - 'v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # Required for Source Link and GitVersion

      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.x'

      - name: Build
        run: dotnet build -c Release --no-incremental

      - name: Test (unit)
        run: dotnet test test/DouglasMQ.Unit.Tests -c Release --no-build

      - name: Test (architecture)
        run: dotnet test test/DouglasMQ.Architecture.Tests -c Release --no-build

      - name: Pack
        run: dotnet pack src/DouglasMQ -c Release --no-build -o ./artifacts

      - name: Push to NuGet
        run: |
          dotnet nuget push artifacts/*.nupkg \
            --api-key ${{ secrets.NUGET_API_KEY }} \
            --source https://api.nuget.org/v3/index.json \
            --skip-duplicate
          dotnet nuget push artifacts/*.snupkg \
            --api-key ${{ secrets.NUGET_API_KEY }} \
            --source https://api.nuget.org/v3/index.json \
            --skip-duplicate
```

Store your NuGet API key in GitHub → Repository Settings → Secrets → `NUGET_API_KEY`.

The workflow triggers on version tags (`v1.0.0`, `v1.2.3`). Tag a release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

---

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
