package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	desc "github.com/Madir/go-metric-stream/pkg/gen/metrics/v1"
	"github.com/Madir/go-metric-stream/services/ingestor/internal/broker/kafka"
	"github.com/Madir/go-metric-stream/services/ingestor/internal/config"
	"github.com/Madir/go-metric-stream/services/ingestor/internal/service"
	transportGRPC "github.com/Madir/go-metric-stream/services/ingestor/internal/transport/grpc"
	"google.golang.org/grpc"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.MustLoad()

	slog.Info("starting ingestor", "grpc_port", cfg.GRPCPath, "kafka_addr", cfg.KafkaAddr)
	producer := kafka.NewProducer([]string{cfg.KafkaAddr}, cfg.KafkaTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("failed to close producer", "err:%w", err)
		}
	}()

	metricService := service.New(producer, log)

	grpcServer := grpc.NewServer()
	desc.RegisterMetricsServiceServer(grpcServer, transportGRPC.NewHandler(metricService))

	l, err := net.Listen("tcp", cfg.GRPCPath)
	if err != nil {
		log.Error("failed to listen", "err:%w", err)
		os.Exit(1)
	}
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			log.Error("gRPC server failed", "err", err)
		}
	}()

	log.Info("ingestor is running")

	<-stop

	log.Info("stopping ingestor...")
	grpcServer.GracefulStop()
	log.Info("ingestor stopped gracefully")

}
