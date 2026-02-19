package domain

import (
	"errors"
	"time"
)

var (
	ErrEmptyDeviceID    = errors.New("device id is empty")
	ErrEmptyMertricName = errors.New("metrics name is empty")
)

type Metric struct {
	DeviceID   string    `json:"device_id"`
	MetricName string    `json:"metric_name"`
	Value      float64   `json:"value"`
	Timestamp  time.Time `json:"time_stamp"`
}

func (m *Metric) ValidateMetric() error {
	if m.DeviceID == "" {
		return ErrEmptyDeviceID
	}
	if m.MetricName == "" {
		return ErrEmptyMertricName
	}
	return nil
}
