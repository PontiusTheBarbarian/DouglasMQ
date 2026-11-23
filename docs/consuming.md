# Consuming Messages

## Implementing a handler

Handlers implement `IHandler<TMessage>` and are decorated with `[Handler]`:

```csharp
[Handler(ExchangeType.direct, "orders.exchange", "order-processing", routingKey: "order.placed")]
public class OrderPlacedHandler : IHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message)
    {
        Console.WriteLine($"Handling order {message.OrderId}");
        return Task.CompletedTask;
    }
}
```

### `[Handler]` attribute parameters

| Parameter | Description |
|-----------|-------------|
| `ExchangeType` | `direct`, `topic`, `fanout`, or `headers` |
| `exchangeName` | Name of the exchange to declare and bind |
| `queueName` | Name of the durable queue to declare |
| `routingKey` | Routing key for the queue binding. Defaults to `"*"` |

The consumer tag (shown in the RabbitMQ management UI) is automatically generated as `{exchangeName}.{queueName}.{routingKey}`.

## Injecting services into handlers

Handlers are plain classes — any dependencies they need must be passed via constructor injection.

### With DI (auto-registration)

All dependencies that are registered in the DI container are resolved automatically:

```csharp
[Handler(ExchangeType.direct, "orders.exchange", "order-notifications", "order.placed")]
public class OrderNotificationHandler : IHandler<OrderPlaced>
{
    private readonly IEmailService _emailService;

    public OrderNotificationHandler(IEmailService emailService)
    {
        _emailService = emailService;
    }

    public async Task HandleAsync(OrderPlaced message)
    {
        await _emailService.SendOrderConfirmationAsync(message.OrderId);
    }
}
```

Handlers are scoped by default: each message is handled in its own DI scope, so scoped dependencies such as a `DbContext` get a fresh instance per message. See [handler lifetimes and scopes](di-integration.md#handler-lifetimes-and-scopes).

Register the dependency first, then auto-register handlers:

```csharp
services.AddScoped<IEmailService, EmailService>();
services.AddDouglasMQ(configuration, typeof(Program), typeof(MySerializer));
```

### Without DI

Pass dependencies directly when constructing the handler:

```csharp
var handler = new OrderNotificationHandler(new EmailService(smtpConfig));
var builder = new DouglasMQBuilder()
    .AddHandler(handler);
```

## Registration strategies

### Auto-registration (recommended)

Scans an assembly for all types with `[HandlerAttribute]` that implement `IHandler<T>`:

**With DI:**
```csharp
// Scans the assembly containing Program for all handlers
services.AddDouglasMQ(configuration, typeof(Program), typeof(MySerializer));
```

**Without DI:**
```csharp
// Provide a factory that resolves handler instances (with or without DI)
new DouglasMQBuilder()
    .AddHandlersFromAssembly<Program>(type => serviceProvider.GetRequiredService(type));
```

### Manual registration

**With DI:**
```csharp
services.AddDouglasMQ(configuration, typeof(MySerializer))
    .AddConsumer<OrderPlaced, OrderPlacedHandler>();
```

**Without DI:**
```csharp
new DouglasMQBuilder()
    .AddHandler(new OrderPlacedHandler())
    .AddHandler(new PaymentProcessedHandler(paymentService));
```

## Exchange types

| Type | Use case |
|------|----------|
| `direct` | Messages routed to queues with an exact routing key match |
| `topic` | Pattern-matching routing keys (e.g. `order.*`, `#.failed`) |
| `fanout` | Broadcast to all bound queues; routing key is ignored |
| `headers` | Route on message header values instead of routing key |

## Lifecycle

Each handler gets one `Consumer` instance which owns one RabbitMQ channel. The channel is created in `Dispatcher.StartAsync` and torn down in `DisposeAsync`. With Microsoft DI, handlers are scoped by default: a new handler instance and DI scope are created for each handling attempt and disposed afterwards. Singleton handlers, and handler instances passed to `DouglasMQBuilder`, live for the whole application.
