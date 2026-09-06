package middleware

import (
	"strconv"
	"time"

	"velocity-engine-control-plane-backend-go/internal/metrics"

	"github.com/gin-gonic/gin"
)

// PrometheusMetrics records http_requests_total and http_request_duration_seconds
// for every request, keyed by the matched Gin route template (c.FullPath()) —
// never the raw request path, which would be unbounded cardinality for
// parameterized routes like /rules/:rule_id. Skips /metrics itself so
// Prometheus's own scrape doesn't inflate its target's request counts.
func PrometheusMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/metrics" {
			c.Next()
			return
		}

		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			// No registered route matched (404) — a fixed placeholder, not
			// the raw path, keeps this bounded.
			route = "unmatched"
		}
		status := strconv.Itoa(c.Writer.Status())
		metrics.HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
	}
}
