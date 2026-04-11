# DouglasMQ

A strongly-typed RabbitMQ wrapper for .NET with built-in retry, dead-lettering, QoS, and Microsoft DI support. Usable with or without a dependency injection container.

```
dotnet add package DouglasMQ
```

---

## Quick Start

**1. Define a message**

```csharp
[Message("orders.exchange", routingKey: "order.placed")]
public class OrderPlaced : IEvent
{
    public Guid MessageId { get; } = Guid.NewGuid();
    public Guid TraceId { get; } = Guid.NewGuid();
    public string OrderId { get; set; } = string.Empty;
}
```

**2. Define a handler**

```csharp
[Handler(ExchangeType.direct, "orders.exchange", "order-processing", routingKey: "order.placed")]
public class OrderPlacedHandler : IHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message)
    {
        Console.WriteLine($"Processing order {message.OrderId}");
        return Task.CompletedTask;
    }
}
```

**3. Wire up and publish** (without DI)

```csharp
var (publisher, consumers) = await new DouglasMQBuilder()
    .Configure(c => c.HostName = "localhost")
    .WithSerializer(new MySerializer())
    .AddHandler(new OrderPlacedHandler())
    .BuildAsync();

await publisher.PublishAsync(new OrderPlaced { OrderId = "ORD-001" });

// On shutdown:
await consumers.DisposeAsync();
await ((IAsyncDisposable)publisher).DisposeAsync();
```

**With Microsoft DI** — see [docs/di-integration.md](docs/di-integration.md).

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Component diagram and design decisions |
| [Getting Started](docs/getting-started.md) | Step-by-step setup guide |
| [Configuration](docs/configuration.md) | All configuration options |
| [Publishing](docs/publishing.md) | Events, commands, and publisher confirms |
| [Consuming](docs/consuming.md) | Handlers, routing, and DI injection |
| [Resilience](docs/resilience.md) | Retry, DLX, QoS, and publisher confirms |
| [Microsoft DI Integration](docs/di-integration.md) | `AddDouglasMQ` extension methods |
| [Without DI](docs/without-di.md) | `DouglasMQBuilder` fluent API |
| [Releasing to NuGet](docs/releasing.md) | Versioning, Source Link, and publish checklist |

---

## References

- [RabbitMQ AMQP best practices](https://stackoverflow.com/questions/32220312/rabbitmq-amqp-best-practice-queue-topic-design-in-a-microservice-architecture)
- [RabbitMQ concepts and best practices](https://medium.com/cwan-engineering/rabbitmq-concepts-and-best-practices-aa3c699d6f08)
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
