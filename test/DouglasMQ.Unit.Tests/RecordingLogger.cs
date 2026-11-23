using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// Captures log entries, and the scopes active when each was written, for assertions.
	/// Usable as an <see cref="ILogger"/> directly or, via <see cref="Provider"/>, inside a host.
	/// </summary>
	internal sealed class RecordingLogger : ILogger
	{
		private readonly AsyncLocalScopes _scopes = new();

		public ConcurrentQueue<Entry> EntryQueue { get; } = new();

		public IReadOnlyList<Entry> Entries => EntryQueue.ToList();

		public ILoggerProvider Provider => new RecordingProvider(this);

		public IDisposable BeginScope<TState>(TState state)
			where TState : notnull
			=> _scopes.Push(state);

		public bool IsEnabled(LogLevel logLevel) => true;

		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
			=> EntryQueue.Enqueue(new Entry(logLevel, formatter(state, exception), exception, _scopes.Snapshot()));

		public sealed record Entry(LogLevel Level, string Message, Exception? Exception, IReadOnlyList<object> Scopes);

		private sealed class RecordingProvider : ILoggerProvider
		{
			private readonly RecordingLogger _logger;

			public RecordingProvider(RecordingLogger logger) => _logger = logger;

			public ILogger CreateLogger(string categoryName) => _logger;

			public void Dispose()
			{
			}
		}

		private sealed class AsyncLocalScopes
		{
			private readonly System.Threading.AsyncLocal<ImmutableStack?> _current = new();

			public IDisposable Push(object state)
			{
				var previous = _current.Value;
				_current.Value = new ImmutableStack(state, previous);
				return new Pop(() => _current.Value = previous);
			}

			public IReadOnlyList<object> Snapshot()
			{
				var result = new List<object>();
				for (var node = _current.Value; node is not null; node = node.Next)
				{
					result.Add(node.State);
				}

				return result;
			}

			private sealed record ImmutableStack(object State, ImmutableStack? Next);

			private sealed class Pop : IDisposable
			{
				private readonly Action _pop;

				public Pop(Action pop) => _pop = pop;

				public void Dispose() => _pop();
			}
		}
	}
}
