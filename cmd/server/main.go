package main

import (
	"context"
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
	"velocity-engine-control-plane-backend-go/internal/services"
	"velocity-engine-control-plane-backend-go/internal/store"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
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
	wsManager := services.NewWSManager()
	anomalyStore := services.NewAnomalyStore(10000)
	anomalyWSMgr := services.NewWSManager()

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

	// Open CSV persistence store (ephemeral local storage for staging)
	csvStore, csvErr := store.Open("/tmp/velocity")
	if csvErr != nil {
		slog.Warn("CSVStore failed to open — rule persistence disabled", "error", csvErr)
		csvStore = nil
	}

	// Create handlers
	rulesHandler := handlers.NewRulesHandler(liveStore, wsManager, csvStore)
	analysisHandler := handlers.NewAnalysisHandler(liveStore, anomalyStore)
	wsHandler := handlers.NewWSHandler(liveStore, wsManager, anomalyStore, anomalyWSMgr)

	// Setup Gin router
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

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

	// Security Headers Middleware
	router.Use(func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("Content-Security-Policy", "default-src 'self'")
		c.Next()
	})

	// Register routes — order matters for Gin!
	// Root & health
	router.GET("/", rulesHandler.ReadRoot)
	router.GET("/health", rulesHandler.Health)
	router.GET("/readyz", rulesHandler.Readyz)

	// Static paths BEFORE parameterized routes to avoid conflicts
	router.GET("/rules/live-analysis", analysisHandler.LiveAnalysis)
	router.GET("/rules/agg-analysis", analysisHandler.AggAnalysis)
	router.GET("/rules/anomaly-analysis", analysisHandler.AnomalyAnalysis)
	router.POST("/rules/historical-test", analysisHandler.HistoricalTest)
	router.POST("/rules/historical-analysis", analysisHandler.HistoricalAnalysis)

	// Rule CRUD
	router.POST("/rules", rulesHandler.CreateRule)
	router.GET("/rules", rulesHandler.ListRules)

	// Parameterized routes AFTER static paths
	router.GET("/rules/:rule_id", rulesHandler.GetRule)
	router.POST("/rules/:rule_id/prod", rulesHandler.PublishRule)
	router.POST("/rules/:rule_id/status", rulesHandler.UpdateRuleStatus)
	router.PUT("/rules/:rule_id", rulesHandler.UpdateRule)
	router.DELETE("/rules/:rule_id", rulesHandler.DeleteRule)
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

	// Close Kafka producer
	services.CloseProducer()

	// Close CSV store (drains write channels)
	if csvStore != nil {
		csvStore.Close()
	}

	// Close ClickHouse
	services.CloseClickHouse()

	slog.Info("Server exited gracefully")
}
