# Publishing

## Events vs Commands

DouglasMQ distinguishes two message semantics:

| | `IEvent` | `ICommand` |
|-|----------|------------|
| Semantics | Something that happened | A request to do something |
| Routing | Pub/sub — many consumers can subscribe | Point-to-point — one consumer handles it |
| Method | `PublishAsync<TEvent>` | `SendAsync<TCommand>` |

Both are published to a RabbitMQ exchange using the same underlying mechanism. The distinction is a design convention enforced by the type system.

## The `[Message]` attribute

Every message class must be decorated with `[Message]`, which specifies the exchange and routing key used when the message is published:

```csharp
[Message(exchangeName: "payments.exchange", routingKey: "payment.processed")]
public class PaymentProcessed : IEvent
{
    public Guid MessageId { get; } = Guid.NewGuid();
    public Guid TraceId { get; } = Guid.NewGuid();
    public string PaymentId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
```

If `routingKey` is omitted it defaults to `"*"`.

## Publishing an event

```csharp
await publisher.PublishAsync(new PaymentProcessed
{
    PaymentId = "PAY-123",
    Amount = 99.99m
});
```

## Sending a command

```csharp
[Message("billing.exchange", "charge.customer")]
public class ChargeCustomer : ICommand
{
    public Guid MessageId { get; } = Guid.NewGuid();
    public Guid TraceId { get; } = Guid.NewGuid();
    public string CustomerId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

await publisher.SendAsync(new ChargeCustomer
{
    CustomerId = "CUST-456",
    Amount = 29.99m
});
```

## Cancellation

Both methods take an optional `CancellationToken`. It cancels opening the connection or channel (on the first publish, or after the channel has been replaced) and waiting for a publisher confirm:

```csharp
await publisher.PublishAsync(new PaymentProcessed { /* ... */ }, cancellationToken);
```

## Publisher confirms

Publisher confirms cause `PublishAsync`/`SendAsync` to wait until the broker has persisted the message before returning. Enable this when message loss is unacceptable (e.g. financial transactions):

```csharp
.Configure(c => c.PublisherConfirms = true)
```

Under the hood, the channel is created with `CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)` using the RabbitMQ.Client 7.x API.

Confirms are **on by default**. To turn them off:

```csharp
.Configure(c => c.PublisherConfirms = false)
```

**Trade-offs:** Publisher confirms have a latency cost because each `PublishAsync` call waits until the broker responds. For high-throughput scenarios without strict durability requirements you can turn them off, at the cost of unroutable messages being logged rather than thrown (see below).

## Unroutable messages

A message the broker cannot route is never dropped silently. `PublishAsync` and `SendAsync` throw `UnroutableMessageException` (with `Exchange`, `RoutingKey`, `ReplyCode` and `ReplyText`) when:

| Case | `ReplyCode` | Confirms on | Confirms off |
|------|-------------|-------------|--------------|
| The exchange does not exist (no consumer has ever started) | `404` (`UnroutableMessageException.NotFound`) | Throws | Throws |
| The exchange exists but no queue is bound for the routing key | `312` (`UnroutableMessageException.NoRoute`) | Throws | Logged at Error; the call returns normally |

How it works:

- **Missing exchange.** Before the first publish to an exchange, the publisher checks once that it exists (a passive declare, on a short-lived channel of its own so the shared publish channel is not closed). The result is cached for the publisher's lifetime; a failed check is not cached, so the send succeeds once a consumer has declared the exchange.
- **No queue bound.** Every message is published with `mandatory: true`, so the broker sends it back (`basic.return`) instead of dropping it. With confirms on, the publish call throws. With confirms off the call has already returned by the time the broker answers, so the return is logged at Error with the message id, exchange and routing key.

This applies to events as well as commands: publishing an event that no queue subscribes to throws (with confirms on). Bind at least one queue, for example by starting its consumer, before publishing.

**The publisher does not declare exchanges.** The consumer side owns the topology (`Handler` attributes declare the exchange, queue and binding). Declaring the exchange from the publisher as well could disagree with the consumer's exchange type (the broker then fails one side with `PRECONDITION_FAILED`), and it would only turn a missing exchange into an unroutable message, which without confirms cannot be reported to the caller.

## MessageId and TraceId

Implement `MessageId` and `TraceId` as **stable properties** — generate them once in the constructor or as `init`-only values. Returning `Guid.NewGuid()` from the property getter creates a new value on every access, making correlation and deduplication impossible:

```csharp
// Correct — stable value
public Guid MessageId { get; } = Guid.NewGuid();

// Wrong — returns a new Guid on every read
public Guid MessageId => Guid.NewGuid();
```
