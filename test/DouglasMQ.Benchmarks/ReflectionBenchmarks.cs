using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DouglasMQ;

namespace DouglasMQ.Benchmarks
{
	/// <summary>
	/// Measures the reflection overhead on the two hot-spots in the current implementation:
	///
	///   1. Consumer.BasicConsume — calls <c>type.GetMethod("HandleAsync")</c> on every message received.
	///   2. Publisher.GetMessageAttribute — calls <c>Attribute.GetCustomAttributes(typeof(T))</c> on every publish.
	///
	/// Each benchmark has an "Uncached" variant (current code) and a "Cached" variant (proposed optimisation)
	/// so the cost difference is immediately visible.
	///
	/// Run with: dotnet run -c Release --project test/DouglasMQ.Benchmarks -- --filter *Reflection*
	/// </summary>
	[MemoryDiagnoser]
	[Orderer(SummaryOrderPolicy.FastestToSlowest)]
	public class ReflectionBenchmarks
	{
		private static readonly Type HandlerType = typeof(BenchmarkHandler);
		private static readonly Type MessageType = typeof(BenchmarkMessage);
		private static readonly MethodInfo CachedHandleMethod = HandlerType.GetMethod("HandleAsync")!;
		private static readonly MessageAttribute CachedAttribute =
			Attribute.GetCustomAttributes(MessageType).OfType<MessageAttribute>().Single();
		private static readonly ConcurrentDictionary<Type, MethodInfo> MethodCache = new();
		private static readonly ConcurrentDictionary<Type, MessageAttribute> AttributeCache = new();
		private static readonly BenchmarkHandler HandlerInstance = new();
		private static readonly BenchmarkMessage MessageInstance = new();

		// ── Handler method lookup ─────────────────────────────────────────────────

		[Benchmark(Baseline = true)]
		public MethodInfo? GetHandleMethod_Uncached()
			=> HandlerType.GetMethod("HandleAsync");

		[Benchmark]
		public MethodInfo GetHandleMethod_Cached()
			=> MethodCache.GetOrAdd(HandlerType, t => t.GetMethod("HandleAsync")!);

		// ── MessageAttribute lookup ───────────────────────────────────────────────

		[Benchmark]
		public MessageAttribute? GetMessageAttribute_Uncached()
			=> Attribute.GetCustomAttributes(MessageType).OfType<MessageAttribute>().SingleOrDefault();

		[Benchmark]
		public MessageAttribute GetMessageAttribute_Cached()
			=> AttributeCache.GetOrAdd(MessageType,
				t => Attribute.GetCustomAttributes(t).OfType<MessageAttribute>().Single());

		// ── Handler invocation ────────────────────────────────────────────────────

		[Benchmark]
		public object? Invoke_ViaReflection()
			=> CachedHandleMethod.Invoke(HandlerInstance, new object[] { MessageInstance });

		[Benchmark]
		public Task Invoke_Direct()
			=> HandlerInstance.HandleAsync(MessageInstance);

		// ── Test types ────────────────────────────────────────────────────────────

		[Message("bench.exchange", "reflect.key")]
		public sealed class BenchmarkMessage : IEvent
		{
			public Guid MessageId { get; } = Guid.NewGuid();
			public Guid TraceId { get; } = Guid.NewGuid();
		}

		[Handler(ExchangeType.direct, "bench.exchange", "bench.queue", "reflect.key")]
		public sealed class BenchmarkHandler : IHandler<BenchmarkMessage>
		{
			public Task HandleAsync(BenchmarkMessage message) => Task.CompletedTask;
		}
	}
}
