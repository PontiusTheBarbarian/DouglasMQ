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

**Trade-offs:** Publisher confirms have a latency cost because each `PublishAsync` call blocks until the broker responds. For high-throughput scenarios without strict durability requirements, leave this `false` (the default).

## MessageId and TraceId

Implement `MessageId` and `TraceId` as **stable properties** — generate them once in the constructor or as `init`-only values. Returning `Guid.NewGuid()` from the property getter creates a new value on every access, making correlation and deduplication impossible:

```csharp
// Correct — stable value
public Guid MessageId { get; } = Guid.NewGuid();

// Wrong — returns a new Guid on every read
public Guid MessageId => Guid.NewGuid();
```
