# Architecture

## Component Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│  Your Application                                                    │
│                                                                      │
│  ┌──────────────┐   PublishAsync / SendAsync   ┌─────────────────┐  │
│  │   IPublisher │ ─────────────────────────►  │    Publisher    │  │
│  │  (interface) │                              │  (sealed class) │  │
│  └──────────────┘                              └────────┬────────┘  │
│                                                         │ IChannel  │
│  ┌──────────────┐  HandleAsync  ┌──────────┐           │           │
│  │  IHandler<T> │ ◄──────────── │ Consumer │ ──────────┤           │
│  │  (your code) │               │(internal)│           │           │
│  └──────────────┘               └────────┬─┘           │           │
│                                          │             │           │
│  ┌──────────────────────────────┐        │ IChannel    │           │
│  │        DouglasMQBuilder      │        │             │           │
│  │  or ServiceCollectionExts    │        │             │           │
│  └────────────────┬─────────────┘        │             │           │
│                   │ IConnection          │             │           │
└───────────────────┼──────────────────────┼─────────────┼───────────┘
                    │                      │             │
                    ▼                      ▼             ▼
          ┌─────────────────────────────────────────────────┐
          │               RabbitMQ Broker                   │
          │                                                 │
          │  ┌───────────────┐    ┌──────────────────────┐  │
          │  │   Exchange    │──► │  Queue (durable)     │  │
          │  │  (your name)  │    │  x-dead-letter-exch  │  │
          │  └───────────────┘    └──────────────────────┘  │
          │                              │ (on NACK)         │
          │  ┌───────────────┐    ┌──────▼──────────────┐   │
          │  │  dlx.exchange │◄───│      dlx.queue      │   │
          │  └───────────────┘    └─────────────────────┘   │
          └─────────────────────────────────────────────────┘
```

## Layers

### Public API (your code depends on these)

| Type | Purpose |
|------|---------|
| `IPublisher` | Contract for publishing events and sending commands |
| `IHandler<TMessage>` | Contract for message consumers |
| `IMessage`, `IEvent`, `ICommand` | Message base types |
| `ISerializer` | Plug-in serialization contract |
| `[Message]` attribute | Declares exchange/routing-key for a message type |
| `[Handler]` attribute | Declares exchange/queue/routing-key for a handler |
| `Configuration` | All broker connection and resilience settings |
| `DouglasMQBuilder` | Fluent builder for DI-free wiring |
| `ServiceCollectionExtensions` | Microsoft DI extension methods |
| `ExchangeType` enum | `direct`, `topic`, `fanout`, `headers` |

### Internal Implementation (opaque to callers)

Both front-ends, `DouglasMQBuilder` and `ServiceCollectionExtensions`, produce only a list of `HandlerRegistration`s. Everything after that is shared, so the two behave identically.

| Type | Purpose |
|------|---------|
| `HandlerRegistration` | One consumer to start: the `[Handler]` binding plus a `HandlerInvoker` |
| `HandlerInvoker` | Strongly-typed call into `IHandler<T>.HandleAsync`, optionally in a new DI scope per message |
| `Dispatcher` | Opens the consumer connection on start, declares the DLX, starts one `Consumer` per registration, and releases everything if startup fails. An `IHostedService` under DI; returned directly by `StartConsumersAsync` |
| `Consumer` | Transport adapter: owns one `IChannel`, applies QoS, and acks/nacks according to the `DeliveryProcessor`'s outcome |
| `DeliveryProcessor` | Body in, `DeliveryOutcome` (`Ack` / `DeadLetter` / `Abandon`) out: deserializes, invokes, retries. No RabbitMQ types |
| `RetryPolicy` | Retry count and capped exponential backoff |
| `Topology` | The only place exchanges, queues and bindings are declared, including the `dlx.*` names |
| `MessageRoute<T>` | `[Message]` routing data, resolved once per message type |
| `ConnectionFactoryBuilder` | Turns `Configuration` into a RabbitMQ connection (TLS, client name) |

Keeping these types internal means their internals can be refactored without breaking any caller.

## RabbitMQ Topology

On startup the `Dispatcher` (or `DouglasMQBuilder.StartConsumersAsync`) declares:

1. **Dead-letter exchange** `dlx.exchange` (direct, durable)
2. **Dead-letter queue** `dlx.queue` (durable), bound with routing key `dlx_routing_key`

Each `Consumer` then declares:

3. **Main exchange** named by `[Handler(exchangeName: "...")]` (durable, your chosen exchange type)
4. **Main queue** named by `[Handler(queueName: "...")]` (durable), configured with `x-dead-letter-exchange = dlx.exchange`
5. **Queue binding** — main exchange → main queue, using the routing key from `[Handler]`

When a handler throws and all retry attempts are exhausted the message is NACKed with `requeue: false`, which causes RabbitMQ to forward it to `dlx.exchange` → `dlx.queue` automatically.

## Design Decisions

### Attribute-driven routing
Routing information (`exchange`, `queue`, `routingKey`) is co-located with the message or handler type rather than configured separately. This makes it impossible to register a handler for a message and forget to specify where to find it.

### DI-optional via `DouglasMQBuilder`
The library works without Microsoft DI so it can be used in console applications, Azure Functions, or unit tests without a full host. The `ServiceCollectionExtensions` are a thin convenience layer over the builder.

### Retry with exponential backoff inside the consumer
Retrying within the consumer (before NACKing) avoids flooding the DLX with transient failures. The delay doubles on each attempt (`retryDelayMs * 2^attempt`) to give downstream systems time to recover.

### Single long-lived channel per publisher
Creating a new `IChannel` per publish (the original bug) was expensive and leaked resources. A single channel with lazy initialization and double-checked locking is safe and efficient for most publishing patterns.

### `IAsyncDisposable` throughout
Both `Publisher` and `Dispatcher` implement `IAsyncDisposable` so channels and connections are closed gracefully. Using `await using` in caller code ensures the broker never sees an abrupt disconnect.
