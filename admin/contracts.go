package admin

// GroupsResponse is the response for listing groups.
type GroupsResponse struct {
	Groups []GroupResponse `json:"groups"`
}

// GroupResponse represents a consumer group.
type GroupResponse struct {
	GroupID   string             `json:"group_id"`
	Consumers []ConsumerResponse `json:"consumers"`
}

// ConsumersResponse is the response for listing consumers.
type ConsumersResponse struct {
	Consumers []ConsumerResponse `json:"consumers"`
}

// ConsumerResponse represents a consumer's status.
type ConsumerResponse struct {
	Name    string   `json:"name"`
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
	Workers int      `json:"workers"`
	Status  string   `json:"status"`
}

// TelemetryResponse is the response for telemetry queries.
type TelemetryResponse struct {
	Groups []TelemetryGroup `json:"groups"`
}

// TelemetryGroup groups telemetry by consumer group.
type TelemetryGroup struct {
	GroupID   string              `json:"group_id"`
	Consumers []TelemetryConsumer `json:"consumers"`
}

// TelemetryConsumer holds telemetry for a consumer.
type TelemetryConsumer struct {
	Name        string                     `json:"name"`
	Workers     int                        `json:"workers"`
	Assignments []TelemetryTopicAssignment `json:"assignments"`
}

// TelemetryTopicAssignment holds telemetry for a topic assignment.
type TelemetryTopicAssignment struct {
	Topic             string  `json:"topic"`
	InstanceName      string  `json:"instance_name"`
	Status            string  `json:"status"`
	RunningPartitions []int32 `json:"running_partitions"`
	PausedPartitions  []int32 `json:"paused_partitions"`
	LastUpdate        int64   `json:"last_update"`
	Lag               int64   `json:"lag"`
}

// ChangeWorkersCountRequest is the request body for changing worker count.
type ChangeWorkersCountRequest struct {
	WorkersCount int `json:"workers_count" binding:"required,gt=0"`
}

// ResetOffsetsRequest is the request body for resetting offsets.
type ResetOffsetsRequest struct {
	Confirm bool `json:"confirm"`
}

// RewindOffsetsToDateRequest is the request body for rewinding offsets.
type RewindOffsetsToDateRequest struct {
	Timestamp int64 `json:"timestamp" binding:"required"`
}
