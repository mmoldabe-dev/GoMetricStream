package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	kafkaConsumer "github.com/Madir/go-metric-stream/services/processor/internal/broker/kafka"
	"github.com/Madir/go-metric-stream/services/processor/internal/config"
	"github.com/Madir/go-metric-stream/services/processor/internal/repository/postgres"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.MustLoad()

	logger.Info("starting processor", "topic", cfg.KafkaTopic, "db_dsn", cfg.PGConnString)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := postgres.NewRepository(ctx, cfg.PGConnString, logger)
	if err != nil {
		logger.Error("Failed to init storage", "err:%w", err)
		os.Exit(1)

	}
	defer repo.Close()

	cosumer := kafkaConsumer.NewConsumer(
		cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, repo, logger)

	defer func() {
		if err := cosumer.Close(); err != nil {
			logger.Error("failed to close consumer", "error", err)
		}
	}()
	go func() {
		if err := cosumer.Start(ctx); err != nil {
			if err != context.Canceled {
				logger.Error("consumer failed", "error", err)
				cancel()
			}
		}
	}()

	go func() {
        http.Handle("/metrics", promhttp.Handler())
        logger.Info("metrics server started", "port", "2112")
        if err := http.ListenAndServe(":2112", nil); err != nil {
            logger.Error("metrics server failed", "error", err)
        }
    }()

	logger.Info("processor is running")

	stop := make(chan os.Signal, 1)

	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop

	logger.Info("stopping processor...")

	cancel()
	logger.Info("processor stopped")
}
