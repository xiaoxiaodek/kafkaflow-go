package consumer_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
)

func TestWorkerBalancerConfig_Defaults(t *testing.T) {
	cfg := consumer.WorkerBalancerConfig{}
	b := consumer.NewWorkerBalancer(nil, cfg)
	assert.NotNil(t, b)
}

func TestWorkerBalancer_StartStop(t *testing.T) {
	cfg := consumer.WorkerBalancerConfig{
		MinWorkers:    1,
		MaxWorkers:    5,
		CheckInterval: 100 * time.Millisecond,
	}
	b := consumer.NewWorkerBalancer(nil, cfg)
	b.Start()
	time.Sleep(50 * time.Millisecond)
	b.Stop()
	// Should not panic
}
