package loggify

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	KindInternal = "internal"
	KindServer   = "server"
	KindClient   = "client"
	KindProducer = "producer"
	KindConsumer = "consumer"
	StatusOK     = "ok"
	StatusError  = "error"
	StatusUnset  = "unset"
)

var traceparentRe = regexp.MustCompile(`(?i)^00-([0-9a-f]{32})-([0-9a-f]{16})-0[01]$`)

type Options struct {
	APIKey          string
	Service         string
	Environment     string
	Endpoint        string
	SampleRate      float64
	FlushIntervalMs int
	MaxBuffer       int
	TimeoutMs       int
	Hostname        string
}

type TraceContext struct {
	TraceID string
	SpanID  string
}

type ctxKey struct{}

type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	kind         string
	startedAt    string
	started      time.Time
	attributes   map[string]any
	name         string
	status       string
	ended        bool
	mu           sync.Mutex
}

func (s *Span) SetName(name string) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.name = clip(name, 512)
	}
	return s
}

func (s *Span) SetAttribute(key string, value any) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.attributes[key] = value
	}
	return s
}

func (s *Span) SetStatus(status string) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	return s
}

func (s *Span) End(status ...string) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	final := s.status
	if len(status) > 0 && status[0] != "" {
		final = status[0]
	}
	name := s.name
	kind := s.kind
	attrs := s.attributes
	startedAt := s.startedAt
	started := s.started
	traceID := s.TraceID
	spanID := s.SpanID
	parent := s.ParentSpanID
	s.mu.Unlock()

	state.mu.RLock()
	opts := state.opts
	state.mu.RUnlock()
	if opts == nil || randFloat() > opts.SampleRate {
		return
	}
	event := map[string]any{
		"traceId":     traceID,
		"spanId":      spanID,
		"name":        name,
		"kind":        kind,
		"status":      final,
		"timestamp":   startedAt,
		"durationMs":  float64(time.Since(started).Microseconds()) / 1000.0,
		"attributes":  attrs,
		"serviceName": opts.Service,
		"environment": opts.Environment,
	}
	if parent != "" {
		event["parentSpanId"] = parent
	}
	state.spanBuf.push(event)
}

type RequestScope struct {
	Span         *Span
	ctx          context.Context
	method       string
	fallbackPath string
	started      time.Time
	statusCode   int
	requestSize  *int
	responseSize *int
	closed       bool
}

func (s *RequestScope) Context() context.Context { return s.ctx }

func (s *RequestScope) SetStatus(statusCode int) { s.statusCode = statusCode }

func (s *RequestScope) SetRequestSize(n int)  { s.requestSize = &n }
func (s *RequestScope) SetResponseSize(n int) { s.responseSize = &n }

func (s *RequestScope) Close() {
	if s.closed {
		return
	}
	s.closed = true
	path := s.fallbackPath
	if route := httpRouteFrom(s.ctx); route != "" {
		path = route
	}
	s.Span.SetAttribute("http.status_code", s.statusCode)
	s.Span.SetAttribute("http.route", path)
	status := StatusOK
	if s.statusCode >= 500 {
		status = StatusError
	}
	s.Span.End(status)
	state.mu.RLock()
	opts := state.opts
	state.mu.RUnlock()
	if opts != nil && randFloat() <= opts.SampleRate {
		event := map[string]any{
			"method":      s.method,
			"route":       path,
			"statusCode":  s.statusCode,
			"durationMs":  float64(time.Since(s.started).Microseconds()) / 1000.0,
			"serviceName": opts.Service,
			"environment": opts.Environment,
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"traceId":     s.Span.TraceID,
		}
		if s.requestSize != nil {
			event["requestSize"] = *s.requestSize
		}
		if s.responseSize != nil {
			event["responseSize"] = *s.responseSize
		}
		state.httpBuf.push(event)
	}
}

type routeKey struct{}

func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKey{}, span)
}

func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(ctxKey{}).(*Span)
	return span
}

func CurrentTraceContext(ctx context.Context) *TraceContext {
	span := SpanFromContext(ctx)
	if span == nil {
		return nil
	}
	return &TraceContext{TraceID: span.TraceID, SpanID: span.SpanID}
}

func SetHTTPRoute(ctx context.Context, route string) {
	span := SpanFromContext(ctx)
	if span == nil {
		return
	}
	span.SetAttribute("http.route", clip(route, 512))
	if ctx != nil {
		// stored on span attributes; also keep for RequestScope via context
	}
	if rs, ok := ctx.Value(routeKey{}).(*string); ok && rs != nil {
		*rs = clip(route, 512)
	}
}

func SetSpanName(ctx context.Context, name string) {
	if span := SpanFromContext(ctx); span != nil {
		span.SetName(name)
	}
}

func SetSpanAttribute(ctx context.Context, key string, value any) {
	if span := SpanFromContext(ctx); span != nil {
		span.SetAttribute(key, value)
	}
}

func httpRouteFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if rs, ok := ctx.Value(routeKey{}).(*string); ok && rs != nil {
		return *rs
	}
	return ""
}

type buffer struct {
	mu    sync.Mutex
	items []map[string]any
	max   int
}

func (b *buffer) push(item map[string]any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max <= 0 {
		b.max = 500
	}
	if len(b.items) >= b.max {
		b.items = b.items[1:]
	}
	b.items = append(b.items, item)
}

func (b *buffer) drain() []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.items
	b.items = nil
	return out
}

type runtimeState struct {
	mu        sync.RWMutex
	opts      *Options
	httpBuf   buffer
	errorBuf  buffer
	metricBuf buffer
	spanBuf   buffer
	client    *http.Client
	started   time.Time
	cancel    context.CancelFunc
}

var state = runtimeState{
	httpBuf:   buffer{max: 500},
	errorBuf:  buffer{max: 500},
	metricBuf: buffer{max: 500},
	spanBuf:   buffer{max: 500},
}

func Init(opts Options) {
	if opts.Endpoint == "" {
		opts.Endpoint = "https://ingest.loggify.cloud"
	}
	opts.Endpoint = strings.TrimRight(opts.Endpoint, "/")
	if opts.SampleRate == 0 {
		opts.SampleRate = 1
	}
	if opts.FlushIntervalMs == 0 {
		opts.FlushIntervalMs = 2000
	}
	if opts.MaxBuffer == 0 {
		opts.MaxBuffer = 500
	}
	if opts.TimeoutMs == 0 {
		opts.TimeoutMs = 1500
	}
	if opts.Hostname == "" {
		opts.Hostname = resolveHostname()
	}
	copied := opts
	state.mu.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	state.opts = &copied
	state.httpBuf.max = opts.MaxBuffer
	state.errorBuf.max = opts.MaxBuffer
	state.metricBuf.max = opts.MaxBuffer
	state.spanBuf.max = opts.MaxBuffer
	state.client = &http.Client{Timeout: time.Duration(opts.TimeoutMs) * time.Millisecond}
	state.started = time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel
	state.mu.Unlock()
	go loopFlush(ctx, time.Duration(opts.FlushIntervalMs)*time.Millisecond)
	go loopRuntime(ctx)
	collectRuntime()
}

func CaptureException(err any, endpoint string, method string, statusCode int) {
	defer func() { _ = recover() }()
	message := fmt.Sprint(err)
	exType := fmt.Sprintf("%T", err)
	if e, ok := err.(error); ok {
		message = e.Error()
	}
	if message == "" {
		message = exType
	}
	payload := map[string]any{
		"message":       message,
		"exceptionType": exType,
		"stackTrace":    string(debug.Stack()),
	}
	if endpoint != "" {
		payload["endpoint"] = endpoint
	}
	if method != "" {
		payload["method"] = method
	}
	if statusCode != 0 {
		payload["statusCode"] = statusCode
	}
	state.errorBuf.push(payload)
	attrs := map[string]any{"exceptionType": exType, "stackTrace": payload["stackTrace"]}
	if endpoint != "" {
		attrs["endpoint"] = endpoint
	}
	if method != "" {
		attrs["method"] = method
	}
	if statusCode != 0 {
		attrs["statusCode"] = statusCode
	}
	logAt(context.Background(), "ERROR", exType+": "+message, attrs)
}

func CaptureExceptionContext(ctx context.Context, err any, endpoint string, method string, statusCode int) {
	defer func() { _ = recover() }()
	message := fmt.Sprint(err)
	exType := fmt.Sprintf("%T", err)
	if e, ok := err.(error); ok {
		message = e.Error()
	}
	payload := map[string]any{
		"message":       message,
		"exceptionType": exType,
		"stackTrace":    string(debug.Stack()),
	}
	if span := SpanFromContext(ctx); span != nil {
		payload["traceId"] = span.TraceID
	}
	if endpoint != "" {
		payload["endpoint"] = endpoint
	}
	if method != "" {
		payload["method"] = method
	}
	if statusCode != 0 {
		payload["statusCode"] = statusCode
	}
	state.errorBuf.push(payload)
	attrs := map[string]any{"exceptionType": exType, "stackTrace": payload["stackTrace"]}
	if endpoint != "" {
		attrs["endpoint"] = endpoint
	}
	logAt(ctx, "ERROR", exType+": "+message, attrs)
}

func StartSpan(ctx context.Context, name string, kind string, attrs map[string]any, parent *TraceContext) *Span {
	var traceID, parentID string
	if parent != nil {
		traceID = parent.TraceID
		parentID = parent.SpanID
	} else if active := SpanFromContext(ctx); active != nil {
		traceID = active.TraceID
		parentID = active.SpanID
	} else {
		traceID = hexBytes(16)
	}
	if kind == "" {
		kind = KindInternal
	}
	copied := map[string]any{}
	for k, v := range attrs {
		copied[k] = v
	}
	return &Span{
		TraceID:      traceID,
		SpanID:       hexBytes(8),
		ParentSpanID: parentID,
		kind:         kind,
		startedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		started:      time.Now(),
		attributes:   copied,
		name:         clip(name, 512),
		status:       StatusUnset,
	}
}

func WithSpan(ctx context.Context, name string, fn func(context.Context, *Span) error) error {
	return WithSpanKind(ctx, name, KindInternal, fn)
}

func WithSpanKind(ctx context.Context, name string, kind string, fn func(context.Context, *Span) error) error {
	span := StartSpan(ctx, name, kind, nil, nil)
	ctx = ContextWithSpan(ctx, span)
	err := fn(ctx, span)
	if err != nil {
		span.End(StatusError)
		return err
	}
	span.End()
	return nil
}

func InjectTraceparent(ctx context.Context) string {
	return InjectTraceparentContext(CurrentTraceContext(ctx))
}

func InjectTraceparentContext(tc *TraceContext) string {
	if tc == nil || tc.TraceID == "" || tc.SpanID == "" {
		return ""
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-01"
}

func ExtractTraceparent(header string) *TraceContext {
	m := traceparentRe.FindStringSubmatch(strings.TrimSpace(header))
	if m == nil {
		return nil
	}
	return &TraceContext{TraceID: strings.ToLower(m[1]), SpanID: strings.ToLower(m[2])}
}

func BeginRequest(ctx context.Context, method, path, traceparent string) *RequestScope {
	parent := ExtractTraceparent(traceparent)
	attrs := map[string]any{"http.method": method, "http.route": path}
	span := StartSpan(ctx, method+" "+path, KindServer, attrs, parent)
	route := path
	ctx = context.WithValue(ContextWithSpan(ctx, span), routeKey{}, &route)
	return &RequestScope{
		Span:         span,
		ctx:          ctx,
		method:       method,
		fallbackPath: path,
		started:      time.Now(),
		statusCode:   200,
	}
}

func Info(message string, attrs map[string]any) { logAt(context.Background(), "INFO", message, attrs) }
func Warn(message string, attrs map[string]any) { logAt(context.Background(), "WARN", message, attrs) }
func Error(message string, attrs map[string]any) {
	logAt(context.Background(), "ERROR", message, attrs)
}
func Debug(message string, attrs map[string]any) {
	logAt(context.Background(), "DEBUG", message, attrs)
}

func InfoContext(ctx context.Context, message string, attrs map[string]any) {
	logAt(ctx, "INFO", message, attrs)
}
func WarnContext(ctx context.Context, message string, attrs map[string]any) {
	logAt(ctx, "WARN", message, attrs)
}
func ErrorContext(ctx context.Context, message string, attrs map[string]any) {
	logAt(ctx, "ERROR", message, attrs)
}

func logAt(ctx context.Context, level, message string, attributes map[string]any) {
	defer func() { _ = recover() }()
	state.mu.RLock()
	opts := state.opts
	state.mu.RUnlock()
	if opts == nil {
		return
	}
	attrs := map[string]any{}
	for k, v := range attributes {
		attrs[k] = v
	}
	if span := SpanFromContext(ctx); span != nil {
		attrs["traceId"] = span.TraceID
		attrs["spanId"] = span.SpanID
	}
	event := map[string]any{
		"level":       level,
		"message":     message,
		"attributes":  attrs,
		"serviceName": opts.Service,
		"environment": opts.Environment,
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
	}
	post("/v1/logs", map[string]any{"logs": []map[string]any{event}}, 0)
}

func Flush() {
	state.mu.RLock()
	opts := state.opts
	state.mu.RUnlock()
	if opts == nil {
		return
	}
	httpRequests := state.httpBuf.drain()
	errors := state.errorBuf.drain()
	metrics := state.metricBuf.drain()
	spans := state.spanBuf.drain()
	if len(httpRequests) == 0 && len(errors) == 0 && len(metrics) == 0 && len(spans) == 0 {
		return
	}
	grouped := map[string][]map[string]any{}
	for _, span := range spans {
		id := fmt.Sprint(span["traceId"])
		copySpan := map[string]any{}
		for k, v := range span {
			if k == "traceId" {
				continue
			}
			copySpan[k] = v
		}
		grouped[id] = append(grouped[id], copySpan)
	}
	traces := make([]map[string]any, 0, len(grouped))
	for id, items := range grouped {
		traces = append(traces, map[string]any{
			"traceId":     id,
			"serviceName": opts.Service,
			"environment": opts.Environment,
			"spans":       items,
		})
	}
	post("/v1/ingest", map[string]any{
		"httpRequests": httpRequests,
		"errors":       errors,
		"metrics":      metrics,
		"traces":       traces,
	}, 0)
}

func IsCollectorURL(url string) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.opts != nil && url != "" && strings.HasPrefix(url, state.opts.Endpoint)
}

func loopFlush(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			Flush()
		}
	}
}

func loopRuntime(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			collectRuntime()
		}
	}
}

func collectRuntime() {
	defer func() { _ = recover() }()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	state.mu.RLock()
	started := state.started
	state.mu.RUnlock()
	pushMetric("memory_usage", float64(mem.Alloc)/1024.0/1024.0)
	pushMetric("heap_used", float64(mem.HeapAlloc)/1024.0/1024.0)
	pushMetric("process_uptime", time.Since(started).Seconds())
}

func pushMetric(name string, value float64) {
	state.mu.RLock()
	opts := state.opts
	state.mu.RUnlock()
	event := map[string]any{"metricName": name, "value": value}
	tags := map[string]string{"pid": strconv.Itoa(os.Getpid())}
	if opts != nil {
		event["serviceName"] = opts.Service
		event["environment"] = opts.Environment
		if opts.Hostname != "" {
			tags["hostname"] = opts.Hostname
		}
	}
	event["tags"] = tags
	state.metricBuf.push(event)
}

func resolveHostname() string {
	if host := strings.TrimSpace(os.Getenv("HOSTNAME")); host != "" {
		return clip(host, 255)
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return clip(strings.TrimSpace(host), 255)
}

func post(path string, body map[string]any, attempt int) {
	state.mu.RLock()
	opts := state.opts
	client := state.client
	state.mu.RUnlock()
	if opts == nil || client == nil {
		return
	}
	go func() {
		payload, err := json.Marshal(body)
		if err != nil {
			return
		}
		req, err := http.NewRequest(http.MethodPost, opts.Endpoint+path, bytes.NewReader(payload))
		if err != nil {
			return
		}
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-api-key", opts.APIKey)
		res, err := client.Do(req)
		if err != nil {
			if attempt < 3 {
				time.Sleep(time.Duration(200*(1<<attempt)) * time.Millisecond)
				post(path, body, attempt+1)
			}
			return
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode == 429 && attempt < 3 {
			time.Sleep(time.Duration(200*(1<<attempt)) * time.Millisecond)
			post(path, body, attempt+1)
		}
	}()
}

func hexBytes(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

func clip(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func randFloat() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 1
	}
	n := uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
	return float64(n) / float64(^uint64(0))
}
