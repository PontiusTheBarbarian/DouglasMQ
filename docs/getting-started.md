# Getting Started

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- A running RabbitMQ instance. The quickest way for local development:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Management UI is then available at `http://localhost:15672` (user: `guest`, password: `guest`).

## Install

```
dotnet add package DouglasMQ
```

## Step 1 — Define a message

A message is a plain C# class decorated with `[Message]` and implementing either `IEvent` (for pub/sub) or `ICommand` (for point-to-point).

```csharp
using DouglasMQ;

[Message("orders.exchange", routingKey: "order.placed")]
public class OrderPlaced : IEvent
{
    // MessageId and TraceId should be stable — set once and never change.
    public Guid MessageId { get; } = Guid.NewGuid();
    public Guid TraceId { get; } = Guid.NewGuid();

    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
```

## Step 2 — Define a handler

```csharp
using DouglasMQ;

[Handler(ExchangeType.direct, "orders.exchange", "order-processing", routingKey: "order.placed")]
public class OrderPlacedHandler : IHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message)
    {
        Console.WriteLine($"Processing order {message.OrderId} for £{message.Amount}");
        return Task.CompletedTask;
    }
}
```

## Step 3 — Choose your wiring approach

### Without DI (console app, Azure Functions, etc.)

```csharp
var (publisher, consumers) = await new DouglasMQBuilder()
    .Configure(c =>
    {
        c.HostName = "localhost";
        c.Username = "guest";
        c.Password = "guest";
    })
    .AddHandler(new OrderPlacedHandler())
    .BuildAsync();

// Publish
await publisher.PublishAsync(new OrderPlaced { OrderId = "ORD-001", Amount = 49.99m });

// Keep alive until shutdown signal, then clean up
Console.CancelKeyPress += async (_, _) =>
{
    await consumers.DisposeAsync();
    await ((IAsyncDisposable)publisher).DisposeAsync();
};
```

### With Microsoft DI (ASP.NET Core, Worker Service)

```csharp
builder.Services.AddDouglasMQ(builder.Configuration, typeof(Program), typeof(SystemTextJsonSerializer));
```

See [di-integration.md](di-integration.md) for the full setup.

## Step 4 — (Optional) Choose a serializer

Message bodies are serialized with the built-in `SystemTextJsonSerializer`, which writes camelCase JSON and reads property names case-insensitively. You don't need to do anything to use it.

To customise it, pass your own `JsonSerializerOptions`:

```csharp
.WithSerializer(new SystemTextJsonSerializer(new JsonSerializerOptions { /* converters, policies */ }))
```

To use another library (Newtonsoft.Json, MessagePack, …), implement `DouglasMQ.Serialization.ISerializer` and register it with `WithSerializer(...)`, or with `AddSerializer(typeof(MySerializer))` in DI. A text serializer only needs `Serialize` and `Deserialize`; DouglasMQ UTF-8 encodes around them. Override `SerializeToBytes` and `DeserializeFromBytes` as well to skip the intermediate string or to use a binary format.

## Next Steps

- [Configuration](configuration.md) — tune retry count, prefetch, publisher confirms
- [Consuming](consuming.md) — auto-register handlers, inject services, fanout routing
- [Resilience](resilience.md) — understand retry backoff, DLX, and QoS
