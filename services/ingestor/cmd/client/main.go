package main

import (
	"context"
	"log"
	"sync"
	"time"

	metrics "github.com/Madir/go-metric-stream/pkg/gen/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address     = "localhost:50051"
	workerCount = 5    // Количество параллельных потоков
	sendCount   = 2000 // Сколько метрик отправит каждый поток
)

func main() {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := metrics.NewMetricsServiceClient(conn)

	var wg sync.WaitGroup
	start := time.Now()

	log.Printf("Starting load test: %d workers, %d metrics each", workerCount, sendCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < sendCount; j++ {
				_, err := client.SendMetric(context.Background(), &metrics.SendMetricRequest{
					DeviceId:   "sensor-load-test",
					MetricName: "cpu_usage",
					Value:      float64(j % 100),
				})
				if err != nil {
					log.Printf("Worker %d error: %v", workerID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	total := workerCount * sendCount
	log.Printf("Done! Sent %d metrics in %v", total, duration)
	log.Printf("Throughput: %.2f req/sec", float64(total)/duration.Seconds())
}
