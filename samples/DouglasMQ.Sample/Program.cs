using DouglasMQ;
using DouglasMQ.Diagnostics;
using DouglasMQ.Sample;
using DouglasMQ.Sample.Handlers;
using DouglasMQ.Sample.Messages;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<SampleOptions>(builder.Configuration.GetSection("Sample"));

// --- DouglasMQ ---------------------------------------------------------------------------
builder.Services.AddDouglasMQ(builder.Configuration, typeof(JsonMessageSerializer), services => services
	.AddConsumer<OrderPlaced, OrderPlacedHandler>()
	.AddConsumer<ShipOrder, ShipOrderHandler>());

builder.Services.AddHostedService<OrderGenerator>();

// --- OpenTelemetry -----------------------------------------------------------------------
// Seq accepts OTLP over HTTP/protobuf at <seq>/ingest/otlp/v1/{logs,traces}.
var otlp = builder.Configuration.GetSection("Otlp");
var otlpEndpoint = (otlp["Endpoint"] ?? "http://localhost:5341/ingest/otlp").TrimEnd('/');
var otlpApiKey = otlp["ApiKey"];
var metricsExporter = otlp["MetricsExporter"] ?? "Console";
var consoleTraces = otlp.GetValue<bool>("ConsoleTraces");

void ConfigureOtlp(OtlpExporterOptions options, string signal)
{
	options.Endpoint = new Uri($"{otlpEndpoint}/v1/{signal}");
	options.Protocol = OtlpExportProtocol.HttpProtobuf;
	if (!string.IsNullOrWhiteSpace(otlpApiKey))
	{
		options.Headers = $"X-Seq-ApiKey={otlpApiKey}";
	}
}

builder.Services.AddOpenTelemetry()
	.ConfigureResource(resource => resource.AddService(SampleTopology.ServiceName))
	.WithTracing(tracing =>
	{
		tracing
			.AddSource(DouglasMQInstrumentation.ActivitySourceName) // DouglasMQ publish/process spans
			.AddSource("RabbitMQ.Client.*")                           // RabbitMQ.Client's own transport spans
			.AddSource(SampleTopology.ServiceName)                    // spans created by this sample
			.AddAspNetCoreInstrumentation()
			.AddOtlpExporter(options => ConfigureOtlp(options, "traces"));

		if (consoleTraces)
		{
			tracing.AddConsoleExporter();
		}
	})
	.WithMetrics(metrics =>
	{
		metrics
			.AddMeter(DouglasMQInstrumentation.MeterName)
			.AddAspNetCoreInstrumentation()
			.AddRuntimeInstrumentation();

		switch (metricsExporter.ToLowerInvariant())
		{
			case "otlp":
				metrics.AddOtlpExporter(options => ConfigureOtlp(options, "metrics"));
				break;
			case "console":
				metrics.AddConsoleExporter((_, reader) => reader.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 30_000);
				break;
		}
	});

builder.Logging.AddOpenTelemetry(logging =>
{
	logging.IncludeFormattedMessage = true;
	logging.IncludeScopes = true;
	logging.AddOtlpExporter(options => ConfigureOtlp(options, "logs"));
});

var app = builder.Build();

// --- Endpoints ---------------------------------------------------------------------------
// Each request starts a trace: HTTP request -> publish -> process OrderPlaced -> publish -> process ShipOrder.
app.MapGet("/", () => Results.Text("""
	DouglasMQ OpenTelemetry sample

	POST /orders                place one order (optional JSON body: { "customer": "Ada", "amount": 42.5 })
	POST /orders/poison         place an order that always fails -> retries -> dead-letter
	POST /orders/burst?count=N  place N orders at once (default 20)

	Orders are also generated automatically every Sample:AutoPublishIntervalSeconds seconds.
	"""));

app.MapPost("/orders", async (IPublisher publisher, Microsoft.Extensions.Options.IOptions<SampleOptions> options, OrderRequest? request) =>
{
	var generated = OrderGenerator.CreateOrder(options.Value, poison: false);
	var order = new OrderPlaced
	{
		OrderId = generated.OrderId,
		Customer = request?.Customer ?? generated.Customer,
		Amount = request?.Amount ?? generated.Amount,
	};

	await publisher.PublishAsync(order);
	return Results.Accepted(value: new { order.OrderId, TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString() });
});

app.MapPost("/orders/poison", async (IPublisher publisher, Microsoft.Extensions.Options.IOptions<SampleOptions> options) =>
{
	var order = OrderGenerator.CreateOrder(options.Value, poison: true);
	await publisher.PublishAsync(order);
	return Results.Accepted(value: new { order.OrderId, TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString() });
});

app.MapPost("/orders/burst", async (IPublisher publisher, Microsoft.Extensions.Options.IOptions<SampleOptions> options, int? count) =>
{
	var orders = Enumerable.Range(0, Math.Clamp(count ?? 20, 1, 1000)).Select(_ => OrderGenerator.CreateOrder(options.Value)).ToList();
	foreach (var order in orders)
	{
		await publisher.PublishAsync(order);
	}

	return Results.Accepted(value: new { Published = orders.Count, TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString() });
});

app.Run();

internal sealed record OrderRequest(string? Customer, decimal? Amount);
