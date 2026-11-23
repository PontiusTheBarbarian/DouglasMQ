# Resilience

## Retry with exponential backoff

When a handler throws an exception, DouglasMQ retries the message automatically before dead-lettering it:

```
Attempt 0 (initial):   executes immediately
Attempt 1 (retry 1):   waits RetryDelayMs × 1   ms  (e.g. 1000 ms)
Attempt 2 (retry 2):   waits RetryDelayMs × 2   ms  (e.g. 2000 ms)
Attempt 3 (retry 3):   waits RetryDelayMs × 4   ms  (e.g. 4000 ms)
→ NACK → dead-letter queue
```

Formula: `delay = RetryDelayMs × 2^(attempt - 1)`

Configure via `Configuration`:

```csharp
c.RetryCount = 5;       // default: 3   (set to 0 to disable retries)
c.RetryDelayMs = 500;   // default: 1000
```

An `OperationCanceledException` thrown by the handler itself (for example an `HttpClient` timeout) is an ordinary failure and is retried. Only cancellation caused by the consumer shutting down stops processing; that delivery is left unacknowledged and redelivered.

## Non-retryable failures

Some failures cannot be fixed by trying again, such as a message that fails validation. Throw `NonRetryableException` to skip the remaining attempts and retry delays:

```csharp
public Task HandleAsync(CreateItem command)
{
    if (command.Quantity <= 0)
    {
        throw new NonRetryableException("Quantity must be positive.");
    }
    // ...
}
```

The handler runs once, the failure is logged at **Warning** (naming the handler, the message type and the reason), and the delivery is NACKed with `requeue: false` straight away, so it reaches `dlx.exchange` → `dlx.queue`.

A `NonRetryableException` also counts when it is wrapped: anywhere in the `InnerException` chain of what the handler throws, including inside an `AggregateException`. Wrapping, by a mediator, a decorator or `Task.WhenAll`, does not make the underlying failure any more retryable.

## Dead-letter exchange (DLX)

All queues managed by DouglasMQ are configured with dead-letter routing:

```
x-dead-letter-exchange  = "dlx.exchange"
x-dead-letter-routing-key = "dlx_routing_key"
```

On startup, DouglasMQ declares:

```
dlx.exchange  (direct, durable)
    └── dlx.queue  (durable) ─── routing key: dlx_routing_key
```

Any message that exhausts all retries is NACKed with `requeue: false`, which causes RabbitMQ to forward it automatically to `dlx.exchange` → `dlx.queue`. Monitor this queue in production to detect persistent failures.

You can bind your own consumer to `dlx.queue` to alert, log, or reprocess dead-lettered messages.

## QoS / Prefetch

Each consumer channel has a prefetch limit configured with `BasicQos`. This controls how many unacknowledged messages RabbitMQ delivers to the consumer at once:

```csharp
c.PrefetchCount = 20;  // default: 10
```

**Too high:** A slow or crashing consumer can hold many messages in-flight, delaying redelivery to other consumers.
**Too low:** Network round-trips dominate and throughput drops.

A value between 5 and 50 is typical for most workloads. Tune based on your message processing time and the number of consumer instances.

## Publisher confirms

Publisher confirms cause each publish to block until the broker acknowledges the message has been written to disk:

```csharp
c.PublisherConfirms = true;  // default: false
```

Use this when:
- Message loss is unacceptable (e.g. payment events, audit logs)
- You are publishing to a durable queue that must survive a broker restart

Do not use this when:
- Latency matters more than durability (e.g. real-time metrics, telemetry)
- You have application-level retry that handles message loss

## Connection auto-recovery

The `ConnectionFactory` is configured with:

```csharp
AutomaticRecoveryEnabled = true,
TopologyRecoveryEnabled = true,
RequestedHeartbeat = TimeSpan.FromSeconds(60),
```

This means RabbitMQ.Client will automatically reconnect and redeclare exchanges, queues, and bindings after a connection drop. No application code is required to handle reconnection.

## Starting while the broker is unreachable

Under `AddDouglasMQ`, a broker outage does not stop the host from starting. The hosted service returns from `StartAsync` at once and sets up the connection, the DLX topology and the consumers in the background. While that keeps failing it retries with a growing delay: 2 s, then doubling, capped at 1 minute. Each failure is logged at **Warning** with the exception and the next delay, and success is logged once at **Information** with the consumer count. Stopping the host ends the retry loop promptly. The publisher is unaffected, because it opens its own connection on first publish.

Inject `IDouglasMQStatus` to report the state, for example in a readiness check:

```csharp
app.MapGet("/health/ready", (IDouglasMQStatus status) =>
    status.ConsumersRunning ? Results.Ok() : Results.StatusCode(503));
```

`DouglasMQBuilder.StartConsumersAsync` behaves differently: it waits until consumers are running and throws if the broker is unreachable, releasing whatever it opened.

## Graceful shutdown

When consumers stop (host shutdown, or disposing the result of `StartConsumersAsync`), each consumer:

1. Cancels its broker subscription, so no new deliveries arrive.
2. Lets handlers that are already running finish and send their ack or nack. Under a host the wait is bounded by the host's shutdown timeout; disposing the result of `StartConsumersAsync` waits up to 30 seconds.
3. Only then closes its channel.

Deliveries not yet started, and those waiting out a retry delay, are left unacknowledged, so the broker requeues them. A handler that finished is never redelivered just because the host was stopping.

Shutdown happens once, however many `StopAsync` and `DisposeAsync` calls overlap (the generic host and `WebApplicationFactory` can stop hosted services twice at once, then dispose the container). Later callers wait for the first shutdown to finish.

