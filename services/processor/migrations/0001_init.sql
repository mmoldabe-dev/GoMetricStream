CREATE TABLE IF NOT EXISTS metrics(
    time TIMESTAMP NOT NULL,
    device_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION NULL
);


SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);


CREATE INDEX IF NOT EXISTS idx_mertic_device_time ON metrics (device_id, metric_name, time DESC);