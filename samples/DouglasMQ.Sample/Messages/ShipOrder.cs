using DouglasMQ;
using DouglasMQ.Messages;

namespace DouglasMQ.Sample.Messages;

[Message(SampleTopology.ShippingExchange, routingKey: "ship.order")]
public sealed class ShipOrder : ICommand
{
	public Guid MessageId { get; init; } = Guid.NewGuid();

	public Guid TraceId { get; init; }

	public string OrderId { get; init; } = string.Empty;
}
