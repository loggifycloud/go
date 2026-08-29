package loggify

import (
	"net/http"
	"strings"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

// Handler records incoming HTTP as a server span. Register it before your mux.
// Go 1.23+ ServeMux patterns (GET /orders/{id}) rewrite the span name after routing.
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "" {
			path = "/"
		}
		scope := BeginRequest(r.Context(), r.Method, path, r.Header.Get("traceparent"))
		ww := &statusWriter{ResponseWriter: w, status: 200}
		r = r.WithContext(scope.Context())
		defer func() {
			if rec := recover(); rec != nil {
				CaptureExceptionContext(r.Context(), rec, path, r.Method, 500)
				scope.SetStatus(500)
				scope.Close()
				panic(rec)
			}
		}()
		next.ServeHTTP(ww, r)
		if r.Pattern != "" {
			pattern := r.Pattern
			if parts := strings.Fields(pattern); len(parts) == 2 {
				pattern = parts[1]
			}
			SetHTTPRoute(scope.Context(), pattern)
			SetSpanName(scope.Context(), r.Method+" "+pattern)
			SetSpanAttribute(scope.Context(), "http.route", pattern)
		}
		scope.SetStatus(ww.status)
		if n := r.ContentLength; n >= 0 {
			scope.SetRequestSize(int(n))
		}
		scope.SetResponseSize(ww.bytes)
		scope.Close()
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Transport wraps a RoundTripper with client spans and W3C traceparent injection.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if IsCollectorURL(req.URL.String()) {
			return base.RoundTrip(req)
		}
		span := StartSpan(req.Context(), "HTTP "+req.Method, KindClient, map[string]any{
			"http.method": req.Method,
			"http.url":    clip(req.URL.String(), 512),
		}, nil)
		ctx := ContextWithSpan(req.Context(), span)
		if header := InjectTraceparent(ctx); header != "" {
			req = req.Clone(ctx)
			req.Header.Set("traceparent", header)
		} else {
			req = req.WithContext(ctx)
		}
		res, err := base.RoundTrip(req)
		if err != nil {
			span.End(StatusError)
			return res, err
		}
		span.SetAttribute("http.status_code", res.StatusCode)
		if res.StatusCode >= 500 {
			span.End(StatusError)
		} else {
			span.End(StatusOK)
		}
		return res, nil
	})
}

// Client returns an HTTP client that records outbound calls as client spans.
func Client() *http.Client {
	return &http.Client{Transport: Transport(http.DefaultTransport)}
}
