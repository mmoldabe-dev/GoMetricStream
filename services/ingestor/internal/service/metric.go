package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Madir/go-metric-stream/services/ingestor/internal/domain"
	
)

type Service struct {
	producer domain.MetricProducer
	logger   *slog.Logger
}

func New(producer domain.MetricProducer, logger *slog.Logger) domain.MetricService {
	return &Service{
		producer: producer,
		logger:   logger,
	}
}

func (s *Service) AcceptMetric(ctx context.Context, metric domain.Metric) error{
	if err := metric.ValidateMetric(); err != nil {
		s.logger.Warn("invalid metric","error", err)
		return fmt.Errorf("validate failed: %w ",err)
	}
	s.logger.Info("processing metric", "device:", metric.DeviceID, "metric:", metric.MetricName)

	if err := s.producer.Send(ctx, metric); err != nil {
		s.logger.Error("failed to send to broker", "error", err)
		return fmt.Errorf("broker error: %w",err)
	}
	return nil
}
