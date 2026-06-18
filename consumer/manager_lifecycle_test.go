package consumer

import "testing"

func TestConsumerManager_StopMarksConsumerStopped(t *testing.T) {
	m := &ConsumerManager{state: stateRunning}

	m.Stop()

	if got := m.Status(); got != "Stopped" {
		t.Fatalf("expected status Stopped after Stop, got %s", got)
	}
}

func TestConsumerManager_ChangeWorkersCountUpdatesWorkerCount(t *testing.T) {
	m := &ConsumerManager{config: ConsumerConfig{WorkerCount: 2}}

	if err := m.ChangeWorkersCount(5); err != nil {
		t.Fatalf("expected no error changing worker count: %v", err)
	}

	if got := m.WorkerCount(); got != 5 {
		t.Fatalf("expected worker count 5, got %d", got)
	}
}

func TestConsumerManager_StatusTransitions(t *testing.T) {
	m := &ConsumerManager{state: stateStopped}
	if got := m.Status(); got != "Stopped" {
		t.Fatalf("expected Stopped, got %s", got)
	}

	m.state = stateStarting
	if got := m.Status(); got != "Starting" {
		t.Fatalf("expected Starting, got %s", got)
	}

	m.state = stateRunning
	if got := m.Status(); got != "Running" {
		t.Fatalf("expected Running, got %s", got)
	}

	m.state = stateStopping
	if got := m.Status(); got != "Stopping" {
		t.Fatalf("expected Stopping, got %s", got)
	}
}

func TestConsumerManager_StopWhenAlreadyStopped(t *testing.T) {
	m := &ConsumerManager{state: stateStopped}
	m.Stop()
	if got := m.Status(); got != "Stopped" {
		t.Fatalf("expected Stopped, got %s", got)
	}
}

func TestConsumerManager_RestartStopsAndStarts(t *testing.T) {
	m := &ConsumerManager{
		config: ConsumerConfig{
			Name:        "test",
			GroupID:     "test-group",
			Topics:      []string{"test"},
			WorkerCount: 1,
			BufferSize:  1,
		},
		state:   stateRunning,
		stopped: make(chan struct{}),
	}
	close(m.stopped)

	if err := m.Restart(); err != nil {
		t.Fatalf("expected no error on restart: %v", err)
	}
}
