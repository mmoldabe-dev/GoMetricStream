package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Madir/go-metric-stream/services/ingestor/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func(p *Producer)Send(ctx context.Context, metric domain.Metric)error{
	data, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("failed to marshal metric: %w",err)

	}

	err = p.writer.WriteMessages(ctx , kafka.Message{
		Key: []byte(metric.DeviceID),
		Value: data,

	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}
	return nil
}

func (p *Producer)Close()error{
	return p.writer.Close()
}
