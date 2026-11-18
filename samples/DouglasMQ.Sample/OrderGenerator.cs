using DouglasMQ.Sample.Messages;
using Microsoft.Extensions.Options;

namespace DouglasMQ.Sample;

/// <summary>
/// Publishes a random order on a timer so there is always telemetry to look at.
/// Each order starts its own trace from the "generate order" span.
/// </summary>
public sealed class OrderGenerator : BackgroundService
{
	private static readonly string[] Customers = ["Ada", "Grace", "Linus", "Margaret", "Ken", "Barbara"];

	private readonly IPublisher _publisher;
	private readonly IOptionsMonitor<SampleOptions> _options;
	private readonly ILogger<OrderGenerator> _logger;

	public OrderGenerator(IPublisher publisher, IOptionsMonitor<SampleOptions> options, ILogger<OrderGenerator> logger)
	{
		_publisher = publisher;
		_options = options;
		_logger = logger;
	}

	public static OrderPlaced CreateOrder(SampleOptions options, bool? poison = null) => new()
	{
		OrderId = $"ORD-{Random.Shared.Next(10000, 99999)}",
		Customer = Customers[Random.Shared.Next(Customers.Length)],
		Amount = Math.Round((decimal)(Random.Shared.NextDouble() * 200 + 5), 2),
		Poison = poison ?? Random.Shared.NextDouble() < options.PoisonRate,
	};

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		// Give the dispatcher time to declare topology before the first publish.
		await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
		{
			var interval = _options.CurrentValue.AutoPublishIntervalSeconds;
			if (interval <= 0)
			{
				await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
				continue;
			}

			try
			{
				using var activity = SampleTopology.ActivitySource.StartActivity("generate order");
				var order = CreateOrder(_options.CurrentValue);
				activity?.SetTag("order.id", order.OrderId);

				await _publisher.PublishAsync(order);
				_logger.LogInformation("Generated order {OrderId} (poison: {Poison}).", order.OrderId, order.Poison);
			}
			catch (Exception ex) when (ex is not OperationCanceledException)
			{
				_logger.LogError(ex, "Failed to publish generated order.");
			}

			await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
		}
	}
}
