package domain

import "time"

type Metric struct {
	DeviceID   string    `json:"device_id" db:"device_id"`
	MetricName string    `json:"metric_name" db:"metric_name"`
	Value      float64   `json:"value" db:"value"`
	Timestamp  time.Time `json:"time_stamp" db:"time"`
}
