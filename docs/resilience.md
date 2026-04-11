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

`OperationCanceledException` is never retried — it propagates immediately to allow clean shutdown.

## Dead-letter exchange (DLX)

All queues managed by DouglasMQ are configured with dead-letter routing:

```
x-dead-letter-exchange  = "dlx.exchange"
x-dead-letter-routing-key = "dlx_routing_key"
```

On startup, `Dispatcher` (or `DouglasMQBuilder.StartConsumersAsync`) declares:

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
