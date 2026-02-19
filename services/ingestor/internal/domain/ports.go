package domain

import "context"

type MetricProducer interface {
	Send(ctx context.Context, metric Metric)error
}
type MetricService interface{
	AcceptMetric(ctx context.Context, metric Metric)error
}