package kafkaConsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Madir/go-metric-stream/services/processor/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

const (
	batchSize    = 1000
	batchTimeout = 5 * time.Second
)

type Consumer struct {
	reader *kafka.Reader
	repo   domain.MetricRepository
	logger *slog.Logger
}

var (
	metricsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_processed_metrics_total",
		Help: "The total number of processed metrics",
	})
)

func NewConsumer(brokers []string, topic string, groupID string, repo domain.MetricRepository, logger *slog.Logger) *Consumer {
	return &Consumer{
		repo:   repo,
		logger: logger,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			MinBytes:    10e3,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting kafka consumer...")

	batch := make([]domain.Metric, 0, batchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 1. Устанавливаем таймаут на чтение (5 секунд)
			// Если сообщений нет 5 секунд, ReadMessage вернет ошибку DeadlineExceeded
			readCtx, cancel := context.WithTimeout(ctx, batchTimeout)
			m, err := c.reader.ReadMessage(readCtx)
			cancel() // Очищаем контекст

			if err != nil {
				// ПРОВЕРКА: Если это просто истекло время ожидания...
				if errors.Is(err, context.DeadlineExceeded) {
					// ...значит, новых сообщений пока нет.
					// Если в буфере что-то есть — сохраняем это в базу прямо сейчас.
					if len(batch) > 0 {
						c.logger.Info("batch timeout reached, flushing buffer", "count", len(batch))
						if err := c.flush(ctx, batch); err != nil {
							c.logger.Error("failed to flush batch on timeout", "error", err)
						}
						batch = batch[:0] // Очищаем буфер
					}
					continue // Идем на следующий круг
				}

				// Если это остановка программы (Ctrl+C)
				if errors.Is(err, context.Canceled) {
					return err
				}

				// Если реальная ошибка сети/Kafka
				c.logger.Error("kafka read error", "error", err)
				time.Sleep(time.Second) // Ждем секунду, чтобы не спамить логами
				continue
			}

			// 2. Если сообщение пришло успешно
			var metric domain.Metric
			if err := json.Unmarshal(m.Value, &metric); err != nil {
				c.logger.Error("failed to unmarshal metric", "error", err)
				continue
			}

			batch = append(batch, metric)

			// 3. Если буфер полон (1000 штук) — сохраняем
			if len(batch) >= batchSize {
				if err := c.flush(ctx, batch); err != nil {
					// Если база упала, возвращаем ошибку, чтобы перезапустить сервис
					return err
				}
				batch = batch[:0]
			}
		}
	}
}

func (c *Consumer) flush(ctx context.Context, batch []domain.Metric) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()

	if err := c.repo.SaveBatch(ctx, batch); err != nil {
		return fmt.Errorf("failed to save batch: %w", err)
	}
	c.logger.Info("batch saved", "count", len(batch), "duration", time.Since(start))
	metricsProcessed.Add(float64(len(batch)))
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
