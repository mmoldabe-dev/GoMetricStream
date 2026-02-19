package config

import "os"

type Config struct {
	GRPCPath   string
	KafkaAddr  string
	KafkaTopic string
}

func MustLoad() *Config {
	return &Config{
		GRPCPath:   getEnv("GRPC_PORT", ":50051"),
		KafkaAddr:  getEnv("KAFKA_ADDR", "localhost:9092"),
		KafkaTopic: getEnv("KAFKA_TOPIC", "metrics_raw"),
	}

}

func getEnv(key, defaultValue string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultValue
}
