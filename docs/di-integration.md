# Microsoft DI Integration

DouglasMQ integrates with `Microsoft.Extensions.DependencyInjection` via extension methods on `IServiceCollection`. The `Dispatcher` is registered as an `IHostedService` so consumers start and stop automatically with the application host.

## Extension method overloads

### Auto-register handlers from an assembly

```csharp
// Scans the assembly containing Program for [Handler] types and registers them as scoped services.
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
| `IPublisher` | Singleton | `Publisher` with its **own** connection, opened on first publish. It keeps working while the host stops, and the container closes it on disposal |
| `Dispatcher` | `IHostedService` | Opens its own consumer connection and starts one consumer per registered handler in the background once the host starts, retrying while the broker is unreachable (see [Resilience](resilience.md#starting-while-the-broker-is-unreachable)); stops them gracefully and closes the connection when it stops. Neither building the container nor starting the host touches the network |
| `IDouglasMQStatus` | Singleton | `ConsumersRunning` tells you whether the consumers are up, e.g. for a readiness check |

The consumer connection is no longer registered as an `IConnection` singleton. To use RabbitMQ directly, open a connection of your own (for example with `DouglasMQBuilder.CreateConnectionAsync`).

## Handler lifetimes and scopes

Handlers are registered as **scoped** by default. DouglasMQ creates a new DI scope for every attempt at handling a message, resolves the handler's dependencies from it, then disposes the handler and the scope once the attempt finishes. That means handlers can safely depend on scoped services such as an EF Core `DbContext`, and a retry never reuses a scoped service that a failed attempt left in a bad state.

| Handler lifetime | Instances | Scope |
|------------------|-----------|-------|
| `Scoped` (default) | New handler per handling attempt | New scope per handling attempt |
| `Transient` | New handler per handling attempt | New scope per handling attempt |
| `Singleton` | One handler for every message | None: it cannot depend on scoped services |

Choose a lifetime when registering:

```csharp
services.AddConsumer<OrderPlaced, OrderPlacedHandler>();                           // scoped
services.AddConsumer<OrderPlaced, AuditHandler>(ServiceLifetime.Singleton);        // one instance
services.AutoRegisterConsumers(typeof(Program), ServiceLifetime.Transient);
```

Handlers you register yourself (`services.AddScoped<IHandler<OrderPlaced>, OrderPlacedHandler>()`, or a factory such as `AddTransient<IHandler<OrderPlaced>>(sp => ...)`) follow the lifetime you give them.

Each scoped or transient handler is created once in a throwaway scope when the host starts. A handler with a missing dependency therefore fails at startup, not on its first message.

## Individual extension methods

```csharp
// Register a single handler manually (scoped unless you pass a lifetime)
services.AddConsumer<OrderPlaced, OrderPlacedHandler>();

// Register only handlers (no full wiring; scoped unless you pass a lifetime)
services.AutoRegisterConsumers(typeof(Program));

// Register a serializer
services.AddSerializer(typeof(MySerializer));
```
