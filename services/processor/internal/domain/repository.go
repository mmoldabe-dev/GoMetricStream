package domain

import (
	"context"
)

type MetricRepository interface {
	SaveBatch(ctx context.Context, metrics []Metric) error
}
