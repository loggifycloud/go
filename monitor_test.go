package loggify

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type collectorPost struct {
	path string
	body string
}

type collector struct {
	mu    sync.Mutex
	posts []collectorPost
}

func (c *collector) add(path, body string) {
	c.mu.Lock()
	c.posts = append(c.posts, collectorPost{path: path, body: body})
	c.mu.Unlock()
}

func (c *collector) all() []collectorPost {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]collectorPost, len(c.posts))
	copy(out, c.posts)
	return out
}

func startCollector(t *testing.T) *collector {
	t.Helper()
	col := &collector{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		col.add(r.URL.Path, string(body))
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)
	Init(Options{
		APIKey:          "test-key",
		Service:         "orders-api",
		Environment:     "test",
		Endpoint:        server.URL,
		FlushIntervalMs: 60_000,
	})
	return col
}

func waitUntil(t *testing.T, col *collector, check func([]collectorPost) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check(col.all()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for collector posts: %v", col.all())
}

func bodies(posts []collectorPost, path string) string {
	var b strings.Builder
	for _, p := range posts {
		if p.path == path {
			b.WriteString(p.body)
		}
	}
	return b.String()
}

func TestRecordsLogsAndExplicitSpans(t *testing.T) {
	col := startCollector(t)

	Info("order accepted", map[string]any{"orderId": "ord_123"})
	Warn("queue delayed", map[string]any{"lagMs": 420})
	waitUntil(t, col, func(p []collectorPost) bool {
		return strings.Contains(bodies(p, "/v1/logs"), "order accepted")
	})

	err := WithSpanKind(context.Background(), "charge", KindClient, func(ctx context.Context, span *Span) error {
		span.SetAttribute("payment.provider", "test")
		got := CurrentTraceContext(ctx)
		if got == nil || got.TraceID != span.TraceID {
			t.Fatalf("expected current context to match span")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	Flush()
	waitUntil(t, col, func(p []collectorPost) bool {
		return strings.Contains(bodies(p, "/v1/ingest"), "charge")
	})
	ingest := bodies(col.all(), "/v1/ingest")
	if !strings.Contains(ingest, `"name":"charge"`) {
		t.Fatalf("missing span name: %s", ingest)
	}
	if !strings.Contains(ingest, `"kind":"client"`) {
		t.Fatalf("missing kind: %s", ingest)
	}
	if !strings.Contains(ingest, "payment.provider") {
		t.Fatalf("missing attribute: %s", ingest)
	}
}

func TestRecordsIncomingHTTPRouteTemplates(t *testing.T) {
	col := startCollector(t)

	scope := BeginRequest(context.Background(), "GET", "/orders/42", "")
	SetHTTPRoute(scope.Context(), "/orders/{id}")
	SetSpanName(scope.Context(), "GET /orders/{id}")
	SetSpanAttribute(scope.Context(), "http.route", "/orders/{id}")
	scope.SetStatus(200)
	scope.Close()
	Flush()
	waitUntil(t, col, func(p []collectorPost) bool {
		return strings.Contains(bodies(p, "/v1/ingest"), "/orders/{id}")
	})
	ingest := bodies(col.all(), "/v1/ingest")
	if !strings.Contains(ingest, `"/orders/{id}"`) {
		t.Fatalf("missing route template: %s", ingest)
	}
	if !strings.Contains(ingest, "GET /orders/{id}") {
		t.Fatalf("missing span name: %s", ingest)
	}
	if strings.Contains(strings.ReplaceAll(ingest, "/orders/{id}", ""), "/orders/42") {
		t.Fatalf("raw path leaked: %s", ingest)
	}
}

func TestCapturesExceptions(t *testing.T) {
	col := startCollector(t)
	CaptureException(errors.New("payment failed"), "/pay", "POST", 500)
	Flush()
	waitUntil(t, col, func(p []collectorPost) bool {
		return strings.Contains(bodies(p, "/v1/ingest"), "payment failed")
	})
	ingest := bodies(col.all(), "/v1/ingest")
	if !strings.Contains(ingest, `"endpoint":"/pay"`) {
		t.Fatalf("missing endpoint: %s", ingest)
	}
}

func TestContinuesW3CTraceparentAcrossHTTPHops(t *testing.T) {
	col := startCollector(t)

	parentTraceID := strings.Repeat("a", 32)
	parentSpanID := strings.Repeat("b", 16)
	got := ExtractTraceparent("00-" + parentTraceID + "-" + parentSpanID + "-01")
	if got == nil || got.TraceID != parentTraceID || got.SpanID != parentSpanID {
		t.Fatalf("extract failed: %#v", got)
	}
	if ExtractTraceparent("nope") != nil {
		t.Fatal("expected invalid header to be rejected")
	}

	var capturedMu sync.Mutex
	var captured []string
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMu.Lock()
		captured = append(captured, r.Header.Get("traceparent"))
		capturedMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(echo.Close)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		client := Client()
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, echo.URL+"/pay", nil)
		res, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		res.Body.Close()
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(Handler(mux))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/orders/1", nil)
	req.Header.Set("traceparent", "00-"+parentTraceID+"-"+parentSpanID+"-01")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	Flush()
	waitUntil(t, col, func(p []collectorPost) bool {
		return strings.Contains(bodies(p, "/v1/ingest"), `"kind":"client"`)
	})
	ingest := bodies(col.all(), "/v1/ingest")
	if !strings.Contains(ingest, parentTraceID) {
		t.Fatalf("missing parent trace: %s", ingest)
	}
	if !strings.Contains(ingest, "GET /orders/{id}") && !strings.Contains(ingest, "GET /orders/1") {
		t.Fatalf("missing server span: %s", ingest)
	}
	capturedMu.Lock()
	headers := append([]string(nil), captured...)
	capturedMu.Unlock()
	if len(headers) != 1 {
		t.Fatalf("expected one outbound header, got %d (%v)", len(headers), headers)
	}
	if !strings.HasPrefix(headers[0], "00-"+parentTraceID+"-") || !strings.HasSuffix(headers[0], "-01") {
		t.Fatalf("bad traceparent %q", headers[0])
	}
}
