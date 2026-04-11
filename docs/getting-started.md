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
    .WithSerializer(new MySerializer())
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
builder.Services.AddDouglasMQ(builder.Configuration, typeof(Program), typeof(MySerializer));
```

See [di-integration.md](di-integration.md) for the full setup.

## Step 4 — Implement a serializer

DouglasMQ does not bundle a serializer so you can use any JSON/binary library:

```csharp
using System.Text.Json;
using DouglasMQ.Serialization;

public class SystemTextJsonSerializer : ISerializer
{
    public string Serialize<T>(T data) => JsonSerializer.Serialize(data);
    public object? Deserialize(string data, Type type) => JsonSerializer.Deserialize(data, type);
}
```

## Next Steps

- [Configuration](configuration.md) — tune retry count, prefetch, publisher confirms
- [Consuming](consuming.md) — auto-register handlers, inject services, fanout routing
- [Resilience](resilience.md) — understand retry backoff, DLX, and QoS
