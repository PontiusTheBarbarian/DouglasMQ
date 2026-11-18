# Using DouglasMQ Without a DI Container

`DouglasMQBuilder` is a fluent builder that creates publishers and starts consumers without requiring any DI framework. This is useful in console applications, Azure Functions, unit tests, and scenarios where you want explicit wiring.

## Fluent API

```csharp
var builder = new DouglasMQBuilder()
    .Configure(c =>
    {
        c.HostName = "localhost";
        c.Username = "guest";
        c.Password = "guest";
        c.RetryCount = 3;
        c.PrefetchCount = 10;
    })
    .WithSerializer(new SystemTextJsonSerializer())
    .WithLoggerFactory(LoggerFactory.Create(b => b.AddConsole()))
    .AddHandler(new OrderPlacedHandler())
    .AddHandler(new PaymentProcessedHandler(new PaymentService()));
```

## Build modes

### Publisher only

```csharp
await using var publisher = (Publisher)await builder.BuildPublisherAsync();
await publisher.PublishAsync(new OrderPlaced { OrderId = "ORD-001" });
// Disposing the publisher also closes its connection
```

### Consumers only

```csharp
await using var consumers = await builder.StartConsumersAsync();
// Consumers run until disposed; disposal also closes their connection
await Task.Delay(Timeout.Infinite, cancellationToken);
```

### Publisher and consumers together

```csharp
var (publisher, consumers) = await builder.BuildAsync();

await publisher.PublishAsync(new OrderPlaced { OrderId = "ORD-001" });

// On shutdown:
await consumers.DisposeAsync();
await ((IAsyncDisposable)publisher).DisposeAsync();
```

## Auto-discover handlers from an assembly

If all your handlers live in one assembly and you have a service locator or factory:

```csharp
var builder = new DouglasMQBuilder()
    .AddHandlersFromAssembly<Program>(type =>
    {
        // Resolve from any container or create directly
        return Activator.CreateInstance(type)!;
    });
```

## Getting a raw connection

Useful when you need to set up topology directly (e.g. for testing):

```csharp
var connection = await builder.CreateConnectionAsync();
// Use connection.CreateChannelAsync() etc.
// Caller is responsible for closing and disposing.
```

## Shutdown pattern

Always dispose consumers before the publisher (to allow in-flight messages to be acknowledged):

```csharp
var cancellationToken = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellationToken.Cancel(); };

var (publisher, consumers) = await builder.BuildAsync();

try
{
    await Task.Delay(Timeout.Infinite, cancellationToken.Token);
}
catch (OperationCanceledException) { }
finally
{
    await consumers.DisposeAsync();
    await ((IAsyncDisposable)publisher).DisposeAsync();
}
```
