using DouglasMQ;
using DouglasMQ.Sample.Messages;
using Microsoft.Extensions.Options;

namespace DouglasMQ.Sample.Handlers;

/// <summary>
/// Simulates order processing: does some work in a child span, fails at random so retries
/// are visible, and sends a <see cref="ShipOrder"/> command so the trace continues to a second consumer.
/// </summary>
[Handler(ExchangeType.direct, SampleTopology.OrdersExchange, SampleTopology.OrdersQueue, routingKey: "order.placed")]
public sealed class OrderPlacedHandler : IHandler<OrderPlaced>
{
	private readonly IPublisher _publisher;
	private readonly ILogger<OrderPlacedHandler> _logger;
	private readonly IOptionsMonitor<SampleOptions> _options;

	public OrderPlacedHandler(IPublisher publisher, ILogger<OrderPlacedHandler> logger, IOptionsMonitor<SampleOptions> options)
	{
		_publisher = publisher;
		_logger = logger;
		_options = options;
	}

	public async Task HandleAsync(OrderPlaced message)
	{
		_logger.LogInformation("Processing order {OrderId} for {Customer} ({Amount:C}).", message.OrderId, message.Customer, message.Amount);

		using (var activity = SampleTopology.ActivitySource.StartActivity("charge payment"))
		{
			activity?.SetTag("order.id", message.OrderId);
			activity?.SetTag("order.amount", (double)message.Amount);

			try
			{
				await Task.Delay(Random.Shared.Next(20, 150));

				if (message.Poison)
				{
					throw new InvalidOperationException($"Order {message.OrderId} is poison and can never be processed.");
				}

				if (Random.Shared.NextDouble() < _options.CurrentValue.FailureRate)
				{
					throw new TimeoutException($"Payment provider timed out for order {message.OrderId}.");
				}
			}
			catch (Exception ex)
			{
				activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
				activity?.AddException(ex);
				throw;
			}
		}

		await _publisher.SendAsync(new ShipOrder { OrderId = message.OrderId, TraceId = message.TraceId });
		_logger.LogInformation("Order {OrderId} charged; shipping requested.", message.OrderId);
	}
}
