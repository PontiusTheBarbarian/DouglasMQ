# DouglasMQ OpenTelemetry sample

A small ASP.NET Core app that shows the traces, logs and metrics DouglasMQ produces.

- `POST /orders` publishes an `OrderPlaced` event.
- `OrderPlacedHandler` "charges payment" in a child span. It fails at random (`Sample:FailureRate`)
  so you can see retries, then sends a `ShipOrder` command.
- `ShipOrderHandler` consumes that command, so one trace spans two consumers.
- Poison orders always fail and end up in `dlx.queue`.
- A background generator places an order every few seconds, so there is always data.

## Run it

1. **RabbitMQ**

   ```bash
   docker compose -f samples/docker-compose.yml up -d
   ```

   If you don't already run Seq, add `--profile seq` to start it too (UI at http://localhost:5341).

2. **Seq.** The sample sends OTLP over HTTP/protobuf to `http://localhost:5341/ingest/otlp`, which
   is Seq's default. If your Seq uses a different port or needs an API key, change `Otlp` in
   `appsettings.json` or use environment variables:

   ```bash
   export Otlp__Endpoint=http://localhost:5341/ingest/otlp
   export Otlp__ApiKey=<seq api key>     # sent as X-Seq-ApiKey
   ```

3. **Start the app**

   ```bash
   dotnet run --project samples/DouglasMQ.Sample
   ```

4. **Make some traffic**

   ```bash
   curl -X POST localhost:5080/orders -H 'content-type: application/json' -d '{"customer":"Ada","amount":42.5}'
   curl -X POST localhost:5080/orders/poison
   curl -X POST "localhost:5080/orders/burst?count=50"
   ```

   Each response includes the `traceId`, which you can paste into Seq's search.

## What to look at in Seq

- **Traces**: open any `POST /orders` or `generate order` trace. You'll see the publish and
  process spans for both hops, and the `charge payment` spans for each attempt.
- **Retries**: search `@Resource.service.name = 'douglasmq-sample' and douglasmq.retry_count > 0`.
  The span events show each failed attempt with its exception.
- **Dead letters**: search `douglasmq.outcome = 'dead_lettered'`.
- **Logs**: handler logs and DouglasMQ's own retry and dead-letter logs carry the trace ID. Click
  through from a log line to its trace.

## Metrics

Seq ingests OTLP logs and traces. By default the sample prints metrics to the console every 30
seconds (`Otlp:MetricsExporter = Console`). If your collector or Seq version accepts OTLP metrics,
set it to `Otlp`; set it to `None` to turn metrics off.

## Settings

| Setting | Default | Meaning |
|---------|---------|---------|
| `DouglasMQ:*` | localhost / guest | Broker connection, retries (`RetryCount`, `RetryDelayMs`), `PublisherConfirms` |
| `Otlp:Endpoint` | `http://localhost:5341/ingest/otlp` | OTLP base URL; `/v1/{traces,logs,metrics}` is appended |
| `Otlp:ApiKey` | empty | Seq API key, sent as `X-Seq-ApiKey` |
| `Otlp:MetricsExporter` | `Console` | `Console`, `Otlp` or `None` |
| `Otlp:ConsoleTraces` | `false` | Also print spans to the console |
| `Sample:AutoPublishIntervalSeconds` | `5` | `0` turns the generator off |
| `Sample:FailureRate` | `0.25` | Chance a handler attempt throws a transient error |
| `Sample:PoisonRate` | `0.05` | Chance a generated order is poison |
