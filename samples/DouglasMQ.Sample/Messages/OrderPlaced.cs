using DouglasMQ;
using DouglasMQ.Messages;

namespace DouglasMQ.Sample.Messages;

[Message(SampleTopology.OrdersExchange, routingKey: "order.placed")]
public sealed class OrderPlaced : IEvent
{
	public Guid MessageId { get; init; } = Guid.NewGuid();

	public Guid TraceId { get; init; } = Guid.NewGuid();

	public string OrderId { get; init; } = string.Empty;

	public string Customer { get; init; } = string.Empty;

	public decimal Amount { get; init; }

	/// <summary>
	/// When true the handler always throws, so the message is retried and then dead-lettered.
	/// </summary>
	public bool Poison { get; init; }
}
