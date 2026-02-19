package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Madir/go-metric-stream/services/processor/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewRepository(ctx context.Context, connString string, logger *slog.Logger) (*Repository, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config:%s, %w", connString, err)
	}
	config.MaxConns = 10
	config.MaxConnIdleTime = 5 * time.Minute

	var pool *pgxpool.Pool
	for i := 0; i < 5; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
		}
		logger.Info("database not ready, waiting...", "attempt", i+1, "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			continue
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db after %d attempts: %w", 5, err)
	}
	repo := &Repository{pool: pool, logger: logger}

	if err := repo.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration failed:%w", err)
	}
	return repo, nil

}

func (r *Repository) Migrate(ctx context.Context) error {
	r.logger.Info("checking database migrations...")

	// SQL скрипт инициализации
	const query = `
	-- 1. Создаем таблицу, если её нет
	CREATE TABLE IF NOT EXISTS metrics (
		time        TIMESTAMPTZ       NOT NULL,
		device_id   TEXT              NOT NULL,
		metric_name TEXT              NOT NULL,
		value       DOUBLE PRECISION  NULL
	);

	-- 2. Превращаем в Hypertable (TimescaleDB)
	-- if_not_exists => TRUE гарантирует, что мы не упадем, если таблица уже гипертаблица
	SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

	-- 3. Создаем композитный индекс для быстрого поиска
	CREATE INDEX IF NOT EXISTS idx_metrics_device_time ON metrics (device_id, time DESC);
	`

	_, err := r.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute migration sql: %w", err)
	}

	r.logger.Info("migrations applied successfully")
	return nil
}

func (r *Repository) SaveBatch(ctx context.Context, metrics []domain.Metric) error {
	rows := make([][]interface{}, len(metrics))

	for i, m := range metrics {
		rows[i] = []interface{}{
			m.Timestamp,
			m.DeviceID,
			m.MetricName,
			m.Value,
		}
	}

	count, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"metrics"},
		[]string{"time", "device_id", "metric_name", "value"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy from failed: %w", err)
	}
	if count != int64(len(metrics)) {
		return fmt.Errorf("expected to insert %d rows, but inserted %d", len(metrics), count)
	}

	return nil

}

func (r *Repository) Close() {
	r.pool.Close()
}
