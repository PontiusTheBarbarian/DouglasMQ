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
| `ClientProvidedName` | `string` | entry assembly name | Name shown in the RabbitMQ management UI under *Connections*. Publisher and consumer connections get `:publisher` / `:consumer` suffixes |
| `Port` | `int` | `-1` | Broker port. `-1` uses the protocol default (5672, or 5671 with TLS) |
| `UseSsl` | `bool` | `false` | Enable TLS. The broker certificate is fully validated unless `SslAcceptablePolicyErrors` says otherwise. No client certificate is needed unless the broker requires mutual TLS |
| `SslServerName` | `string?` | `HostName` | Name expected on the broker's certificate. Set this when connecting by IP or through a load balancer |
| `SslClientCertificate` | `string?` | `null` | Optional Base64-encoded client certificate for mutual TLS: PKCS#12 (`.pfx`, keeps the private key) or DER/PEM. Formerly `SslCertificateThumbprint`, which still works (and still binds from configuration) but is obsolete |
| `SslCertificatePassword` | `string?` | `null` | Password for a PKCS#12 client certificate |
| `SslAcceptablePolicyErrors` | `SslPolicyErrors` | `None` | Certificate errors to tolerate. Leave as `None` in production; relaxing it disables man-in-the-middle protection |
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

Server-authenticated TLS (the common case) needs only `UseSsl`:

```csharp
.Configure(c =>
{
    c.HostName = "rabbitmq.internal";
    c.UseSsl = true;
})
```

The broker's certificate is validated against `SslServerName` (defaulting to `HostName`) and the machine's trusted roots.

For mutual TLS, also supply a client certificate. Use a PKCS#12 file so the private key is included:

```csharp
c.SslClientCertificate = Convert.ToBase64String(File.ReadAllBytes("client.pfx"));
c.SslCertificatePassword = "pfx-password";
```

For local development against a self-signed broker certificate you can relax validation explicitly. Never ship this to production:

```csharp
c.SslAcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors;
```
