using DouglasMQ;
using DouglasMQ.Sample.Messages;

namespace DouglasMQ.Sample.Handlers;

[Handler(ExchangeType.direct, SampleTopology.ShippingExchange, SampleTopology.ShippingQueue, routingKey: "ship.order")]
public sealed class ShipOrderHandler : IHandler<ShipOrder>
{
	private readonly ILogger<ShipOrderHandler> _logger;

	public ShipOrderHandler(ILogger<ShipOrderHandler> logger) => _logger = logger;

	public async Task HandleAsync(ShipOrder message)
	{
		await Task.Delay(Random.Shared.Next(10, 60));
		_logger.LogInformation("Order {OrderId} shipped.", message.OrderId);
	}
}
