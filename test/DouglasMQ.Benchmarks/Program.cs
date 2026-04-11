using BenchmarkDotNet.Running;

// Run with: dotnet run -c Release --project test/DouglasMQ.Benchmarks
// To run a specific benchmark: dotnet run -c Release --project test/DouglasMQ.Benchmarks -- --filter *Serialization*
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
