# Configuration

All settings live in the `DouglasMQ.Configuration` class. When using Microsoft DI the section is read from `IConfiguration` under the key `"DouglasMQ"`.

## appsettings.json example

```json
{
  "DouglasMQ": {
    "HostName": "rabbitmq.internal",
    "Username": "app",
    "Password": "s3cr3t",
    "VirtualHost": "/orders",
    "PrefetchCount": 20,
    "RetryCount": 5,
    "RetryDelayMs": 500,
    "PublisherConfirms": false
  }
}
```

## All properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `HostName` | `string` | `"localhost"` | RabbitMQ broker hostname or IP address |
| `Username` | `string` | `"guest"` | RabbitMQ username |
| `Password` | `string` | `"guest"` | RabbitMQ password |
| `VirtualHost` | `string` | `"/"` | RabbitMQ virtual host |
| `ClientProvidedName` | `string` | entry assembly name | Name shown in the RabbitMQ management UI under *Connections* |
| `UseSsl` | `bool` | `false` | Enable TLS for the broker connection |
| `SslCertificateThumbprint` | `string?` | `null` | Base64-encoded X.509 certificate bytes, required when `UseSsl` is `true` |
| `PrefetchCount` | `ushort` | `10` | Maximum number of unacknowledged messages delivered to each consumer channel at once. Lower values reduce memory pressure; higher values increase throughput. |
| `RetryCount` | `int` | `3` | Number of retry attempts before a message is dead-lettered. Set to `0` to disable retries. |
| `RetryDelayMs` | `int` | `1000` | Base delay in milliseconds between retry attempts. The actual delay for attempt *n* is `RetryDelayMs × 2^(n-1)` (exponential backoff). |
| `PublisherConfirms` | `bool` | `false` | When `true`, the publisher waits for the broker to confirm persistence of each message before `PublishAsync` / `SendAsync` returns. Has a performance cost — use only when at-least-once delivery is critical. |

## Programmatic configuration (without DI)

```csharp
var builder = new DouglasMQBuilder()
    .Configure(c =>
    {
        c.HostName = "rabbitmq.internal";
        c.Username = "app";
        c.Password = "s3cr3t";
        c.PrefetchCount = 20;
        c.RetryCount = 5;
        c.RetryDelayMs = 500;
    });
```

## SSL / TLS

```csharp
.Configure(c =>
{
    c.HostName = "rabbitmq.internal";
    c.UseSsl = true;
    c.SslCertificateThumbprint = Convert.ToBase64String(File.ReadAllBytes("cert.pfx"));
})
```

The certificate is loaded via `X509CertificateLoader.LoadCertificate` (the modern .NET 9+ API). The connection factory is configured to accept `RemoteCertificateNameMismatch` and `RemoteCertificateChainErrors` — review these for production deployments.
