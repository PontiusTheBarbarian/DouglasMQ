# Observability (OpenTelemetry)

DouglasMQ emits traces and metrics through .NET's built-in `System.Diagnostics` APIs
(`ActivitySource` and `Meter`). The library does **not** depend on the OpenTelemetry SDK. Your
application chooses whether to collect the telemetry and where to send it.

| Signal  | Source name | Constant |
|---------|-------------|----------|
| Traces  | `DouglasMQ` | `DouglasMQInstrumentation.ActivitySourceName` |
| Metrics | `DouglasMQ` | `DouglasMQInstrumentation.MeterName` |
| Logs    | your `ILoggerFactory` | — |

When nothing is listening, the instrumentation does almost no work.

## Enabling it

```csharp
using DouglasMQ.Diagnostics;

builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("orders-service"))
    .WithTracing(t => t
        .AddSource(DouglasMQInstrumentation.ActivitySourceName)
        .AddSource("RabbitMQ.Client.*")      // optional: RabbitMQ.Client's own transport spans
        .AddOtlpExporter())
    .WithMetrics(m => m
        .AddMeter(DouglasMQInstrumentation.MeterName)
        .AddOtlpExporter());

builder.Logging.AddOpenTelemetry(l => l.AddOtlpExporter());
```

Packages: `OpenTelemetry.Extensions.Hosting` and `OpenTelemetry.Exporter.OpenTelemetryProtocol`.

Log records written while a DouglasMQ span is active, including DouglasMQ's own retry and
dead-letter logs and anything your handler logs, carry that span's trace and span ID. You can
jump from a log line straight to its trace.

Around each handler attempt DouglasMQ also opens a logger scope with `MessageId` and `TraceId`.
Loggers from the same `ILoggerFactory`, including the ones injected into your handlers, include them
when the provider records scopes (for example `AddJsonConsole(o => o.IncludeScopes = true)`).

## Traces

### Context propagation

When a message is published, DouglasMQ writes the current trace context into the AMQP message
headers (`traceparent`, `tracestate` and `baggage`, the W3C format by default; it follows
`DistributedContextPropagator.Current`). The consumer reads it back, so a trace started by an HTTP
request continues through every hop:

```
POST /orders                                   (ASP.NET Core)
└─ publish sample.orders                       (DouglasMQ, Producer)
   └─ publish order.placed                     (RabbitMQ.Client, optional)
      └─ deliver order.placed                  (RabbitMQ.Client, optional)
         └─ process sample.order-processing    (DouglasMQ, Consumer: the delivery)
            └─ sample.order-processing process (DouglasMQ, Consumer: one per handler attempt)
               ├─ charge payment               (your handler code)
               └─ publish sample.shipping      (DouglasMQ, Producer)
                  └─ …
```

If RabbitMQ.Client's `deliver` span is active and belongs to the same trace, the `process` span
nests under it. Otherwise it is parented directly on the publisher's context.

### Span: `publish {exchange}` (Producer)

| Attribute | Example |
|-----------|---------|
| `messaging.system` | `rabbitmq` |
| `messaging.operation.name` / `.type` | `publish` / `send` |
| `messaging.destination.name` | exchange name |
| `messaging.rabbitmq.destination.routing_key` | `order.placed` |
| `messaging.message.id` | `IMessage.MessageId` |
| `messaging.message.conversation_id` | `IMessage.TraceId` |
| `messaging.message.body.size` | bytes |
| `douglasmq.message.type` | `OrderPlaced` |
| `douglasmq.publisher_confirms` | `true` / `false` |

If the publish fails, the span gets status `Error`, an `exception` event and `error.type`.

The span covers waiting for the broker confirm when `PublisherConfirms` is enabled.

### Span: `process {queue}` (Consumer)

The span covers deserialization, every handler attempt and the final ack or nack.

| Attribute | Example |
|-----------|---------|
| `messaging.operation.name` / `.type` | `process` / `process` |
| `messaging.destination.name` | exchange name |
| `messaging.destination.subscription.name` | queue name |
| `messaging.rabbitmq.destination.routing_key` | routing key of the delivery |
| `messaging.rabbitmq.message.delivery_tag` | delivery tag |
| `messaging.rabbitmq.message.redelivered` | `true` / `false` |
| `messaging.message.id` / `.conversation_id` | from the deserialized message |
| `douglasmq.handler` | `OrderPlacedHandler` |
| `douglasmq.message.type` | `OrderPlaced` |
| `douglasmq.retry_count` | number of retries used |
| `douglasmq.outcome` | `ack`, `dead_lettered`, or `requeued` (left for redelivery because the consumer was shutting down or the channel closed) |
| `error.type` | exception type when dead-lettered |

Events on the span:

- `exception`: one per failed handler attempt, tagged with `douglasmq.attempt`.
- `douglasmq.retry`: before each retry, tagged with `douglasmq.attempt` and `douglasmq.retry.delay_ms`.

A dead-lettered message sets the span status to `Error`. A message that succeeds after retries
keeps status `Unset`, but its retry history is still visible in the events.

### Span: `{queue} process` (Consumer, one per handler attempt)

Each handler attempt, retries included, gets its own span as a child of the delivery's `process {queue}`
span, so a retried message shows each attempt with its own timing and error.

| Attribute | Example |
|-----------|---------|
| `messaging.system` | `rabbitmq` |
| `messaging.operation.name` / `.type` | `process` / `process` |
| `messaging.destination.name` | exchange name |
| `messaging.rabbitmq.destination.routing_key` | routing key of the delivery |
| `messaging.message.id` / `.conversation_id` | from the deserialized message |
| `messaging.consumer.group.name` | queue name (also as `messaging.destination.subscription.name`) |
| `douglasmq.handler` / `douglasmq.message.type` | `OrderPlacedHandler` / `OrderPlaced` |
| `douglasmq.attempt` | `1` for the first attempt, `2` for the first retry, … |

A failed attempt has status `Error`, an `exception` event and `error.type`. A
`NonRetryableException` ends the delivery after that one failed attempt.

### Adding your own spans

Spans you start inside `HandleAsync` automatically become children of the attempt's `{queue} process` span:

```csharp
private static readonly ActivitySource Source = new("Orders");

public async Task HandleAsync(OrderPlaced message)
{
    using var activity = Source.StartActivity("charge payment");
    // …
}
```

Remember to `AddSource("Orders")` in your tracing setup.

### `IMessage.TraceId` vs. trace context

`IMessage.TraceId` is part of your message contract and travels in the body. It is recorded as
`messaging.message.conversation_id` and sent as the AMQP `correlation_id` property. It does
**not** drive trace propagation; the headers do. This means messages from publishers that don't use
OpenTelemetry still work, and your business correlation ID stays independent of the trace.

## Metrics

| Instrument | Type | Unit | Tags |
|------------|------|------|------|
| `douglasmq.messages.published` | Counter | `{message}` | system, destination, message type |
| `douglasmq.publish.duration` | Histogram | `s` | system, destination, message type, `error.type` on failure |
| `douglasmq.messages.consumed` | Counter | `{message}` | system, destination, subscription, handler, message type, `douglasmq.outcome`, `error.type` |
| `douglasmq.process.duration` | Histogram | `s` | same as consumed |
| `douglasmq.handler.retries` | Counter | `{retry}` | system, destination, subscription, handler, message type |
| `douglasmq.messages.dead_lettered` | Counter | `{message}` | same as consumed |

The duration histograms use the bucket boundaries the OpenTelemetry semantic conventions recommend
for seconds (5 ms up to 10 s).

Useful queries:

- **Failure rate**: `messages.consumed{outcome="dead_lettered"} / messages.consumed`
- **Flaky handlers**: `handler.retries` by `douglasmq.handler`
- **Slow handlers**: p95 of `process.duration` by `douglasmq.handler`

## Trying it out

[`samples/DouglasMQ.Sample`](../samples/DouglasMQ.Sample) is a small web app wired up as above. It
exports to Seq, generates orders, and fails some of them on purpose so you can see retries and
dead-lettering.
