# Observability

## Tracing

DouglasMQ emits spans through an `ActivitySource` named `DouglasMQInstrumentation.ActivitySourceName` (`"DouglasMQ"`). Add it to OpenTelemetry:

```csharp
services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(DouglasMQInstrumentation.ActivitySourceName));
```

### Process spans

Every **handling attempt** gets its own span, so retries show up as sibling spans:

| Field | Value |
|-------|-------|
| Name | `<queue> process` |
| Kind | `Consumer` |
| `messaging.system` | `rabbitmq` |
| `messaging.operation.type` | `process` |
| `messaging.destination.name` | The exchange the message was published to |
| `messaging.rabbitmq.destination.routing_key` | The routing key |
| `messaging.message.id` | The message's `MessageId` |
| `messaging.message.conversation_id` | The message's `TraceId` |
| `messaging.consumer.group.name` | The queue |
| `douglasmq.handler`, `douglasmq.message.type`, `douglasmq.attempt` | Handler class, message type, attempt number (from 1) |

A failed attempt has status `Error`, an `error.type` tag and an `exception` event.

The span joins the sender's trace:

1. If the message carries a W3C `traceparent` header (DouglasMQ's publisher always sets one when there is a current activity), that context is the parent.
2. Otherwise the message's `TraceId` is used as the trace id. A `Guid` and a W3C trace id are both 16 bytes, so the trace id is the `TraceId` itself; the parent span id is synthetic, derived from the `MessageId`.
3. If an activity in the same trace is already current (for example RabbitMQ.Client's own receive span), the process span nests under it.

### Publish spans

`PublishAsync` and `SendAsync` create a `Producer` span named `<exchange> publish` and write `traceparent` (and `tracestate` / `baggage`) into the message headers.

## Log context

Around each handling attempt, DouglasMQ opens a logger scope with `MessageId` and `TraceId`. Loggers created by the same `ILoggerFactory`, including the ones injected into your handlers, include them when the provider records scopes (for example `builder.Logging.AddJsonConsole(o => o.IncludeScopes = true)`).
