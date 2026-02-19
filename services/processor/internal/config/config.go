package config

import (
	"os"
	"strings"
)

type Config struct {
	PGConnString string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
}

func MustLoad() *Config {
	return &Config{
		PGConnString: getEnv("PG_DSN", "postgres://user:pass@GoMetricsPostgres:5432/metrics_db?sslmode=disable"),

		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "GoMetricsKafka:9092"), ","),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "metrics_raw"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "metrics_processor_group"),
	}
}

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}
