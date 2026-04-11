# Microsoft DI Integration

DouglasMQ integrates with `Microsoft.Extensions.DependencyInjection` via extension methods on `IServiceCollection`. The `Dispatcher` is registered as an `IHostedService` so consumers start and stop automatically with the application host.

## Extension method overloads

### Auto-register handlers from an assembly

```csharp
// Scans the assembly containing Program for [Handler] types and registers them as singletons.
services.AddDouglasMQ(configuration, assemblyMarker: typeof(Program), serializer: typeof(MySerializer));
```

This is the recommended approach for most applications.

### Manual registration with optional extra services

```csharp
services.AddDouglasMQ(configuration, serializer: typeof(MySerializer), configure: services =>
{
    // Any additional registrations needed by your handlers
    services.AddScoped<IMyService, MyService>();
});
// Then manually register each consumer
services.AddConsumer<OrderPlaced, OrderPlacedHandler>();
```

### Delegate-based configuration

```csharp
services.AddDouglasMQ((config, services) =>
{
    config.HostName = "rabbitmq.internal";
    config.PrefetchCount = 20;
    services.AddSingleton<ISerializer, MySerializer>();
    services.AddConsumer<OrderPlaced, OrderPlacedHandler>();
});
```

## Complete `Program.cs` example (Worker Service)

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddLogging()
    .AddDouglasMQ(builder.Configuration, typeof(Program), typeof(SystemTextJsonSerializer));

// Any services your handlers depend on
builder.Services.AddScoped<IEmailService, SmtpEmailService>();

var host = builder.Build();
host.Run();
```

`appsettings.json`:
```json
{
  "DouglasMQ": {
    "HostName": "localhost",
    "Username": "guest",
    "Password": "guest",
    "RetryCount": 3,
    "PrefetchCount": 10
  }
}
```

## What gets registered

| Service | Lifetime | Description |
|---------|----------|-------------|
| `Configuration` | Singleton | Resolved configuration values |
| `IConnection` | Singleton | RabbitMQ connection (lazy — created on first resolve) |
| `List<Consumer>` | Singleton | All discovered/registered consumer instances |
| `IPublisher` | Singleton | `Publisher` instance for publishing messages |
| `Dispatcher` | `IHostedService` | Starts consumers on host start, stops on host stop |

## Individual extension methods

```csharp
// Register a single handler manually
services.AddConsumer<OrderPlaced, OrderPlacedHandler>();

// Register only handlers (no full wiring)
services.AutoRegisterConsumers(typeof(Program));

// Register a serializer
services.AddSerializer(typeof(MySerializer));
```
