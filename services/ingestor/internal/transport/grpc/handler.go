package grpc

import (
	"context"
	"time"

	desc "github.com/Madir/go-metric-stream/pkg/gen/metrics/v1"
	"github.com/Madir/go-metric-stream/services/ingestor/internal/domain"
)

type Handlers struct {
	desc.UnimplementedMetricsServiceServer
	service domain.MetricService
}

func NewHandler(service domain.MetricService) *Handlers {
	return &Handlers{
		service: service,
	}
}

func (h *Handlers) SendMetric(ctx context.Context, req *desc.SendMetricRequest) (*desc.SendMetricResponse, error) {
	var ts time.Time

	if req.GetTimestamp() != nil {
		ts = req.GetTimestamp().AsTime()
	} else {
		ts = time.Now()
	}

	metric := domain.Metric{
		DeviceID:   req.GetDeviceId(),
		MetricName: req.GetMetricName(),
		Value:      req.GetValue(),
		Timestamp:  ts,
	}

	if err := h.service.AcceptMetric(ctx, metric); err != nil {
		return &desc.SendMetricResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &desc.SendMetricResponse{
		Success: true,
		Message: "Metric accepted",
	}, nil
}
