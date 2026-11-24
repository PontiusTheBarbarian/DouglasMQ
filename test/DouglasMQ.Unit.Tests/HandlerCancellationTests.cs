using System;
using System.Threading;
using System.Threading.Tasks;
using DouglasMQ.Messages;
using Xunit;

namespace DouglasMQ.Unit.Tests
{
	/// <summary>
	/// R7: handlers can take a <see cref="CancellationToken"/>; handlers written against the
	/// one-argument <c>HandleAsync</c> keep working unchanged.
	/// </summary>
	public class HandlerCancellationTests
	{
		[Fact]
		public async Task TokenAwareHandler_ReceivesTheToken()
		{
			var handler = new TokenAwareHandler();
			var invoker = HandlerInvoker.Create(handler);
			using var cts = new CancellationTokenSource();

			await invoker.InvokeAsync(new Message(), cts.Token);

			Assert.Equal(cts.Token, handler.Received);
		}

		[Fact]
		public async Task LegacyHandler_IsStillCalled()
		{
			var handler = new LegacyHandler();
			var invoker = HandlerInvoker.Create(handler);

			await invoker.InvokeAsync(new Message(), new CancellationTokenSource().Token);

			Assert.Equal(1, handler.Calls);
		}

		[Fact]
		public async Task CallingTheOneArgumentOverload_OnATokenAwareHandler_PassesNone()
		{
			var handler = new TokenAwareHandler();

			await ((IHandler<Message>)handler).HandleAsync(new Message());

			Assert.Equal(CancellationToken.None, handler.Received);
		}

		[Fact]
		public void HandlerImplementingNeitherOverload_IsRejectedUpFront()
		{
			var ex = Assert.Throws<InvalidOperationException>(() => HandlerInvoker.Create(new EmptyHandler()));

			Assert.Contains(nameof(EmptyHandler), ex.Message, StringComparison.Ordinal);
		}

		[Fact]
		public void ExplicitImplementationOfTheTokenOverload_IsAccepted()
		{
			HandlerInvoker.Create(new ExplicitTokenHandler());
		}

		public sealed class Message : IMessage
		{
			public Guid MessageId { get; } = Guid.NewGuid();

			public Guid TraceId { get; } = Guid.NewGuid();
		}

		private sealed class TokenAwareHandler : IHandler<Message>
		{
			public CancellationToken? Received { get; private set; }

			public Task HandleAsync(Message message, CancellationToken cancellationToken)
			{
				Received = cancellationToken;
				return Task.CompletedTask;
			}
		}

		private sealed class LegacyHandler : IHandler<Message>
		{
			public int Calls { get; private set; }

			public Task HandleAsync(Message message)
			{
				Calls++;
				return Task.CompletedTask;
			}
		}

		private sealed class EmptyHandler : IHandler<Message>
		{
		}

		private sealed class ExplicitTokenHandler : IHandler<Message>
		{
			Task IHandler<Message>.HandleAsync(Message message, CancellationToken cancellationToken) => Task.CompletedTask;
		}
	}
}
