using System;

namespace DouglasMQ
{
	/// <summary>
	/// Thrown by a handler to say that retrying cannot help, for example because the message fails
	/// validation. DouglasMQ then makes no further attempts and waits out no retry delays: it logs a
	/// warning and dead-letters the delivery straight away, so it reaches <c>dlx.exchange</c> / <c>dlx.queue</c>.
	/// </summary>
	/// <remarks>
	/// The exception also counts when it is wrapped: when it appears anywhere in the
	/// <see cref="Exception.InnerException"/> chain of the exception a handler throws, including among
	/// the inner exceptions of an <see cref="AggregateException"/>. Wrapping (by a mediator,
	/// <c>Task.WhenAll</c> or a decorator) does not make the underlying failure any more retryable.
	/// </remarks>
	public class NonRetryableException : Exception
	{
		/// <summary>Initialises a new <see cref="NonRetryableException"/>.</summary>
		public NonRetryableException()
		{
		}

		/// <summary>Initialises a new <see cref="NonRetryableException"/> with a message explaining why retrying cannot help.</summary>
		/// <param name="message">Why the message cannot be handled. Logged when the delivery is dead-lettered.</param>
		public NonRetryableException(string message)
			: base(message)
		{
		}

		/// <summary>Initialises a new <see cref="NonRetryableException"/> wrapping the failure that caused it.</summary>
		/// <param name="message">Why the message cannot be handled. Logged when the delivery is dead-lettered.</param>
		/// <param name="innerException">The underlying failure.</param>
		public NonRetryableException(string message, Exception innerException)
			: base(message, innerException)
		{
		}

		/// <summary>
		/// Finds a <see cref="NonRetryableException"/> in <paramref name="exception"/> or its inner exceptions.
		/// </summary>
		internal static NonRetryableException? Find(Exception? exception)
		{
			// Bounded so a cyclic or absurdly deep chain cannot hang a consumer.
			for (var depth = 0; exception is not null && depth < 32; depth++)
			{
				switch (exception)
				{
					case NonRetryableException nonRetryable:
						return nonRetryable;
					case AggregateException aggregate:
						foreach (var inner in aggregate.InnerExceptions)
						{
							if (Find(inner) is { } found)
							{
								return found;
							}
						}

						return null;
				}

				exception = exception.InnerException;
			}

			return null;
		}
	}
}
