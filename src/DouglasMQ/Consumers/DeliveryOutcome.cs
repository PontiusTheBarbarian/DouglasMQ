namespace DouglasMQ
{
	/// <summary>
	/// What should happen to a delivery once DouglasMQ has finished processing it.
	/// </summary>
	internal enum DeliveryOutcome
	{
		/// <summary>The handler succeeded: acknowledge the delivery.</summary>
		Ack,

		/// <summary>The message is poison or the handler kept failing: reject it without requeueing, so the broker dead-letters it.</summary>
		DeadLetter,

		/// <summary>The consumer is shutting down mid-processing: leave the delivery unsettled so the broker redelivers it.</summary>
		Abandon,
	}
}
