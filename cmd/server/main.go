package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/handlers"
	"velocity-engine-control-plane-backend-go/internal/metrics"
	"velocity-engine-control-plane-backend-go/internal/middleware"
	"velocity-engine-control-plane-backend-go/internal/services"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Configure structured logging
	logLevel := slog.LevelInfo
	switch strings.ToUpper(config.LogLevel) {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN", "WARNING":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("Starting Velocity Engine Control Plane",
		"host", config.ServerHost,
		"port", config.ServerPort,
	)

	// Set Gin mode based on log level
	if logLevel > slog.LevelDebug {
		gin.SetMode(gin.ReleaseMode)
	}

	// Initialize services
	liveStore := services.NewLiveStore()
	wsManager := services.NewWSManager("live")
	anomalyStore := services.NewAnomalyStore(10000)
	anomalyWSMgr := services.NewWSManager("anomaly")

	metrics.RegisterLiveStoreCollectors(liveStore.TotalRows, liveStore.DroppedRowsTotal)
	metrics.RegisterMySQLPoolCollectors(services.MySQLPoolStats)

	// Bootstrap LiveStore from ClickHouse
	func() {
		slog.Info("Bootstrapping LiveStore from ClickHouse...")
		bootstrapData, err := services.GetLiveResultsMulti([]string{}, config.LiveStoreHours)
		if err != nil {
			slog.Error("Failed to bootstrap LiveStore from ClickHouse", "error", err)
			return
		}
		var allRows []map[string]interface{}
		for _, rows := range bootstrapData {
			allRows = append(allRows, rows...)
		}
		liveStore.Bootstrap(allRows)
		slog.Info("LiveStore bootstrapped", "stats", liveStore.StatsString())
	}()

	// Start Kafka results consumer in background
	consumer := services.NewResultsConsumer(liveStore, wsManager)
	consumer.Start()

	// Start Kafka anomaly consumer in background
	anomalyConsumer := services.NewAnomalyConsumer(anomalyStore, anomalyWSMgr)
	anomalyConsumer.Start()

	// MySQL: verify the required schema already exists (RBAC tables + the
	// five rule-definition storage tables). This backend
	// never creates, alters, or seeds this schema — it's provisioned
	// manually per environment (see internal/migrations/mysql/*.sql for the
	// DDL to run by hand). Non-fatal by design, same as the ClickHouse/DuckDB
	// init paths: RBAC-protected routes fail closed (503), and rule
	// creation/edits fail closed (503) with a clear error, until the schema
	// is confirmed present — but the rest of the backend keeps running.
	func() {
		verifyCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := services.VerifyMySQLSchema(verifyCtx); err != nil {
			slog.Error("MySQL schema verification failed — RBAC-protected routes and rule persistence will be unavailable until this is resolved", "error", err)
		}
	}()
	authMW := middleware.NewAuthMiddleware(services.GetUserPermissions)

	// Wires IdentityMiddleware's real resolver dependency (see that
	// function's doc comment for the current trust model). The not-found
	// translation here is the one place services.ErrUserNotFound and
	// middleware.ErrIdentityNotFound meet — see both sentinels' doc comments
	// for why internal/middleware doesn't import internal/services directly.
	middleware.SetIdentityResolver(func(ctx context.Context, sub string) (int64, string, error) {
		userID, status, err := services.GetUserByExternalSubject(ctx, sub)
		if errors.Is(err, services.ErrUserNotFound) {
			return 0, "", middleware.ErrIdentityNotFound
		}
		return userID, status, err
	})

	// Create handlers
	rulesHandler := handlers.NewRulesHandler(liveStore, wsManager)

	// Reload the rule list from MySQL — this is what lets the backend
	// survive a restart with GET /rules still showing what was there before,
	// instead of coming back empty (the previous CSV-based rule store never
	// supported reading its own history back). Non-fatal: if MySQL is down,
	// rulesDB just starts empty and self-heals as rules are recreated/edited.
	func() {
		loadCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		rulesHandler.LoadFromMySQL(loadCtx)
	}()

	analysisHandler := handlers.NewAnalysisHandler(liveStore, anomalyStore)
	wsHandler := handlers.NewWSHandler(liveStore, wsManager, anomalyStore, anomalyWSMgr)
	iamHandler := handlers.NewIAMHandler()

	// Setup Gin router
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(middleware.PrometheusMetrics())

	// Request body size limit (1MB)
	router.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20) // 1 MB
		c.Next()
	})

	// CORS middleware
	corsOrigins := strings.Split(config.CORSOrigins, ",")
	var allowOrigins []string
	for i := range corsOrigins {
		trimmed := strings.TrimSpace(corsOrigins[i])
		if trimmed != "" {
			allowOrigins = append(allowOrigins, trimmed)
		}
	}
	allowAll := len(allowOrigins) == 1 && allowOrigins[0] == "*"

	router.Use(cors.New(cors.Config{
		AllowOrigins:     allowOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"*"},
		AllowCredentials: !allowAll,
		MaxAge:           12 * time.Hour,
	}))

	// Security headers (X-Content-Type-Options, X-Frame-Options,
	// Referrer-Policy, Content-Security-Policy, Permissions-Policy) are set
	// once, at the edge, by the frontend's nginx (see nginx.conf) for every
	// path including the /api/ proxy_pass — this backend is never reached
	// directly by a browser. Setting a second, weaker copy here caused every
	// proxied response to carry duplicate headers (e.g. two
	// Content-Security-Policy headers, which browsers merge into one comma-
	// joined value) — a real finding from the 2026-07-23 WAS scan.

	// Register routes — order matters for Gin!
	// Root & health
	router.GET("/", rulesHandler.ReadRoot)
	router.GET("/health", rulesHandler.Health)
	router.GET("/readyz", rulesHandler.Readyz)
	// Unauthenticated, same as /health and /readyz — Prometheus scrapes this
	// via annotation-based discovery (no request-level auth, matching the
	// platform's existing pattern), not through the frontend's WSO2 flow.
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{})))

	// Identity / authorization — frontend calls this once at bootstrap to
	// hydrate its authorization context (which UI elements to show/hide).
	router.GET("/me", middleware.IdentityMiddleware(), iamHandler.Me)

	// Static paths BEFORE parameterized routes to avoid conflicts
	router.GET("/rules/live-analysis", analysisHandler.LiveAnalysis)
	router.GET("/rules/agg-analysis", analysisHandler.AggAnalysis)
	router.GET("/rules/anomaly-analysis", analysisHandler.AnomalyAnalysis)
	router.POST("/rules/historical-test", analysisHandler.HistoricalTest)
	router.POST("/rules/historical-analysis", analysisHandler.HistoricalAnalysis)
	router.POST("/rules/historical-breakdown", analysisHandler.HistoricalBreakdown)

	// Rule CRUD
	router.POST("/rules", middleware.IdentityMiddleware(), authMW.RequirePermission("rules", "create"), rulesHandler.CreateRule)
	router.GET("/rules", rulesHandler.ListRules)

	// Parameterized routes AFTER static paths
	router.GET("/rules/:rule_id", rulesHandler.GetRule)
	// Mutations are gated behind RequirePermission using the resource:action
	// keys seeded in 0001_init_rbac.sql. UpdateRuleStatus is gated on
	// "publish" (not a separate key) because setting status to ACTIVE calls
	// the same services.PublishRule path as PublishRule itself — it's an
	// alternate route to the identical production-activation effect, so it
	// must require the identical permission, not a lesser one. Read-only
	// routes (GET /rules, GET /rules/:rule_id, GET /rules/:rule_id/live-results)
	// remain intentionally unguarded — that's a broader policy decision than
	// this pass, left for a follow-up.
	router.POST("/rules/:rule_id/prod", middleware.IdentityMiddleware(), authMW.RequirePermission("rules", "publish"), rulesHandler.PublishRule)
	router.POST("/rules/:rule_id/status", middleware.IdentityMiddleware(), authMW.RequirePermission("rules", "publish"), rulesHandler.UpdateRuleStatus)
	router.PUT("/rules/:rule_id", middleware.IdentityMiddleware(), authMW.RequirePermission("rules", "update"), rulesHandler.UpdateRule)
	router.DELETE("/rules/:rule_id", middleware.IdentityMiddleware(), authMW.RequirePermission("rules", "delete"), rulesHandler.DeleteRule)
	router.GET("/rules/:rule_id/live-results", rulesHandler.LiveResults)

	// WebSocket routes
	router.GET("/ws/live-analysis", wsHandler.LiveAnalysisWS)
	router.GET("/ws/anomaly-analysis", wsHandler.AnomalyAnalysisWS)

	// Create HTTP server
	// WriteTimeout is set to 15 min to support long-running DuckDB historical analysis queries
	// and ClickHouse aggregation scans. This matches the nginx proxy_read_timeout on the frontend.
	addr := fmt.Sprintf("%s:%s", config.ServerHost, config.ServerPort)
	srv := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadTimeout:       15 * time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      15 * time.Minute,
		IdleTimeout:       5 * time.Minute,
	}

	// Start server in goroutine
	go func() {
		slog.Info("Server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("Received shutdown signal", "signal", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop consumers
	consumer.Stop()
	anomalyConsumer.Stop()

	// Shutdown HTTP server
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Server forced to shutdown", "error", err)
	}

	// srv.Shutdown does not track or wait for hijacked connections, which is
	// exactly what every active WebSocket connection is (see
	// internal/handlers/websocket.go's Upgrade call) — without this, an
	// in-flight Live Stream/Anomaly panel connection would just die when
	// this process exits below, with no close frame ever sent to the
	// client. CloseAll sends each one a real close frame first.
	wsManager.CloseAll()
	anomalyWSMgr.CloseAll()

	// Close Kafka producer
	services.CloseProducer()

	// Close ClickHouse
	services.CloseClickHouse()

	// Close MySQL
	services.CloseMySQL()

	slog.Info("Server exited gracefully")
}
