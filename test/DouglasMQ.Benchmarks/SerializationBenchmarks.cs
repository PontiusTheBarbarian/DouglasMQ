using System;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DouglasMQ;
using DouglasMQ.Serialization;

namespace DouglasMQ.Benchmarks
{
	/// <summary>
	/// Measures the cost of message serialization and deserialization.
	/// Serialization runs on every publish and every consume — it is in the critical hot path.
	///
	/// Run with: dotnet run -c Release --project test/DouglasMQ.Benchmarks -- --filter *Serialization*
	/// </summary>
	[MemoryDiagnoser]
	[Orderer(SummaryOrderPolicy.FastestToSlowest)]
	public class SerializationBenchmarks
	{
		private readonly SystemTextJsonSerializer _serializer = new();
		private readonly SmallMessage _small = new() { Name = "benchmark" };
		private readonly LargeMessage _large = new()
		{
			Name = "benchmark",
			Description = "A longer description field to simulate a realistic message",
			Value = 42,
			Count = 100,
			IsActive = true,
			Tag1 = "alpha",
			Tag2 = "beta",
			Tag3 = "gamma",
			Timestamp = DateTimeOffset.UtcNow,
			CorrelationId = Guid.NewGuid().ToString(),
		};
		private string _serializedSmall = string.Empty;

		[GlobalSetup]
		public void Setup()
		{
			_serializedSmall = _serializer.Serialize(_small);
		}

		[Benchmark]
		public string Serialize_SmallMessage() => _serializer.Serialize(_small);

		[Benchmark]
		public string Serialize_LargeMessage() => _serializer.Serialize(_large);

		[Benchmark]
		public object? Deserialize_SmallMessage() => _serializer.Deserialize(_serializedSmall, typeof(SmallMessage));

		[Benchmark]
		public object? RoundTrip()
		{
			var json = _serializer.Serialize(_small);
			return _serializer.Deserialize(json, typeof(SmallMessage));
		}

		// --- Serializer implementation ---

		private sealed class SystemTextJsonSerializer : ISerializer
		{
			public object? Deserialize(string data, Type type) => JsonSerializer.Deserialize(data, type);
			public string Serialize<T>(T data) => JsonSerializer.Serialize(data);
		}

		// --- Message types ---

		[Message("bench.exchange", "small.key")]
		public sealed class SmallMessage : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public string Name { get; set; } = string.Empty;
		}

		[Message("bench.exchange", "large.key")]
		public sealed class LargeMessage : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
			public string Name { get; set; } = string.Empty;
			public string Description { get; set; } = string.Empty;
			public int Value { get; set; }
			public long Count { get; set; }
			public bool IsActive { get; set; }
			public string Tag1 { get; set; } = string.Empty;
			public string Tag2 { get; set; } = string.Empty;
			public string Tag3 { get; set; } = string.Empty;
			public DateTimeOffset Timestamp { get; set; }
			public string CorrelationId { get; set; } = string.Empty;
		}
	}
}
