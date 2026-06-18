package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *Server) registerDashboardRoutes(r *gin.RouterGroup) {
	r.GET("/consumers/telemetry", s.getDashboardTelemetry)
}

func (s *Server) getDashboardTelemetry(c *gin.Context) {
	if s.storage == nil {
		c.JSON(http.StatusOK, DashboardTelemetryResponse{Groups: []DashboardConsumerGroup{}})
		return
	}

	metrics := s.storage.GetAll()

	groupMap := make(map[string]*DashboardConsumerGroup)
	for _, m := range metrics {
		group, ok := groupMap[m.GroupID]
		if !ok {
			group = &DashboardConsumerGroup{
				GroupID:   m.GroupID,
				Consumers: []DashboardConsumer{},
			}
			groupMap[m.GroupID] = group
		}

		var found bool
		for i, tc := range group.Consumers {
			if tc.Name == m.ConsumerName {
				group.Consumers[i].Assignments = append(group.Consumers[i].Assignments, DashboardTopicPartitionAssignment{
					InstanceName:      m.InstanceName,
					Lag:               m.Lag,
					Workers:           m.WorkersCount,
					LastUpdate:        formatLastUpdate(m.SentAt),
					PausedPartitions:  m.PausedPartitions,
					RunningPartitions: m.RunningPartitions,
					Status:            m.Status,
					TopicName:         m.Topic,
				})
				found = true
				break
			}
		}
		if !found {
			group.Consumers = append(group.Consumers, DashboardConsumer{
				Name: m.ConsumerName,
				Assignments: []DashboardTopicPartitionAssignment{{
					InstanceName:      m.InstanceName,
					Lag:               m.Lag,
					Workers:           m.WorkersCount,
					LastUpdate:        formatLastUpdate(m.SentAt),
					PausedPartitions:  m.PausedPartitions,
					RunningPartitions: m.RunningPartitions,
					Status:            m.Status,
					TopicName:         m.Topic,
				}},
			})
		}
	}

	var groups []DashboardConsumerGroup
	for _, g := range groupMap {
		groups = append(groups, *g)
	}

	c.JSON(http.StatusOK, DashboardTelemetryResponse{Groups: groups})
}

func formatLastUpdate(sentAt int64) string {
	if sentAt <= 0 {
		return ""
	}
	return time.UnixMilli(sentAt).UTC().Format("2006-01-02T15:04:05")
}

type DashboardTelemetryResponse struct {
	Groups []DashboardConsumerGroup `json:"groups"`
}

type DashboardConsumerGroup struct {
	GroupID   string              `json:"groupId"`
	Consumers []DashboardConsumer `json:"consumers"`
}

type DashboardConsumer struct {
	Name        string                              `json:"name"`
	Assignments []DashboardTopicPartitionAssignment `json:"assignments"`
}

type DashboardTopicPartitionAssignment struct {
	InstanceName      string  `json:"instanceName"`
	Lag               int64   `json:"lag"`
	Workers           int     `json:"workers"`
	LastUpdate        string  `json:"lastUpdate"`
	PausedPartitions  []int32 `json:"pausedPartitions"`
	RunningPartitions []int32 `json:"runningPartitions"`
	Status            string  `json:"status"`
	TopicName         string  `json:"topicName"`
}
