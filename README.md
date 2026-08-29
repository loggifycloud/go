# loggify-go

Go monitoring SDK for Loggify. Incoming HTTP is captured with `loggify.Handler`; outbound calls through `loggify.Client()` or `loggify.Transport` become client spans. Logs, errors, traces, and runtime metrics are posted as Loggify JSON to ingest.

Call `loggify.Init` **before** serving HTTP.

```go
import (
    "os"

    "github.com/loggifycloud/go"
)

func main() {
    loggify.Init(loggify.Options{
        APIKey:      os.Getenv("LOGGIFY_KEY"),
        Service:     "orders-api",
        Environment: "production",
        Endpoint:    os.Getenv("LOGGIFY_ENDPOINT"),
    })
}
```

## Install

```bash
go get github.com/loggifycloud/go
```

From this repository:

```bash
go get github.com/loggifycloud/go@v0.1.0
```

## HTTP

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /orders/{id}", ordersShow)
http.ListenAndServe(":8080", loggify.Handler(mux))
```

Go 1.23+ ServeMux patterns show up as `GET /orders/{id}`. Incoming `traceparent` continues a distributed trace. Outbound calls inject the **client** span as W3C `traceparent`:

```go
client := loggify.Client()
res, err := client.Get("https://pay.example/charge")
```

## Logs

Use `InfoContext` (and friends) inside a handler so the log keeps the request's trace id:

```go
loggify.InfoContext(r.Context(), "order accepted", map[string]any{"orderId": "ord_123"})
loggify.Warn("queue delayed", map[string]any{"lagMs": 420})
loggify.Error("payment failed", map[string]any{"provider": "stripe"})
```

## Errors

```go
if err != nil {
    loggify.CaptureExceptionContext(r.Context(), err, "/pay", "POST", 500)
    return
}
```

HTTP 5xx panics from `loggify.Handler` are captured automatically.

## Traces

```go
_ = loggify.WithSpan(r.Context(), "charge", func(ctx context.Context, span *loggify.Span) error {
    span.SetAttribute("order.id", order.ID)
    return charge(ctx, order)
})

header := loggify.InjectTraceparent(r.Context()) // 00-{traceId}-{spanId}-01
parent := loggify.ExtractTraceparent(incoming)
```

Datastore queries are not auto-patched. Wrap work with `WithSpan` or send OTLP from existing instrumentations to the same ingest URL.
