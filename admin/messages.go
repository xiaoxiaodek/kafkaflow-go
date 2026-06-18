package admin

// AdminMessage is the marker interface for all admin command messages.
type AdminMessage interface {
	isAdminMessage()
}

// PauseConsumerByName pauses a specific consumer.
type PauseConsumerByName struct {
	ConsumerName string   `json:"consumer_name"`
	Topics       []string `json:"topics"`
}

func (PauseConsumerByName) isAdminMessage() {}

// ResumeConsumerByName resumes a specific consumer.
type ResumeConsumerByName struct {
	ConsumerName string   `json:"consumer_name"`
	Topics       []string `json:"topics"`
}

func (ResumeConsumerByName) isAdminMessage() {}

// StartConsumerByName starts a stopped consumer.
type StartConsumerByName struct {
	ConsumerName string `json:"consumer_name"`
}

func (StartConsumerByName) isAdminMessage() {}

// StopConsumerByName stops a running consumer.
type StopConsumerByName struct {
	ConsumerName string `json:"consumer_name"`
}

func (StopConsumerByName) isAdminMessage() {}

// RestartConsumerByName restarts a consumer.
type RestartConsumerByName struct {
	ConsumerName string `json:"consumer_name"`
}

func (RestartConsumerByName) isAdminMessage() {}

// ResetConsumerOffset resets consumer offsets to the beginning.
type ResetConsumerOffset struct {
	ConsumerName string   `json:"consumer_name"`
	Topics       []string `json:"topics"`
}

func (ResetConsumerOffset) isAdminMessage() {}

// RewindConsumerOffsetToDateTime rewinds offsets to a specific timestamp.
type RewindConsumerOffsetToDateTime struct {
	ConsumerName string   `json:"consumer_name"`
	Timestamp    int64    `json:"timestamp"`
	Topics       []string `json:"topics"`
}

func (RewindConsumerOffsetToDateTime) isAdminMessage() {}

// ChangeConsumerWorkersCount changes the number of workers for a consumer.
type ChangeConsumerWorkersCount struct {
	ConsumerName string `json:"consumer_name"`
	WorkersCount int    `json:"workers_count"`
}

func (ChangeConsumerWorkersCount) isAdminMessage() {}

// PauseConsumersByGroup pauses all consumers in a group.
type PauseConsumersByGroup struct {
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

func (PauseConsumersByGroup) isAdminMessage() {}

// ResumeConsumersByGroup resumes all consumers in a group.
type ResumeConsumersByGroup struct {
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

func (ResumeConsumersByGroup) isAdminMessage() {}

// ConsumerTelemetryMetric is a telemetry data point (not an admin command).
type ConsumerTelemetryMetric struct {
	GroupID           string  `json:"group_id"`
	ConsumerName      string  `json:"consumer_name"`
	Topic             string  `json:"topic"`
	InstanceName      string  `json:"instance_name"`
	RunningPartitions []int32 `json:"running_partitions"`
	PausedPartitions  []int32 `json:"paused_partitions"`
	SentAt            int64   `json:"sent_at"`
	WorkersCount      int     `json:"workers_count"`
	Status            string  `json:"status"`
	Lag               int64   `json:"lag"`
}
