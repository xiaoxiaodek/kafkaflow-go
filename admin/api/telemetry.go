package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
)

func (s *Server) getTelemetry(c *gin.Context) {
	if s.storage == nil {
		c.JSON(http.StatusOK, admin.TelemetryResponse{Groups: []admin.TelemetryGroup{}})
		return
	}

	metrics := s.storage.GetAll()

	groupMap := make(map[string]*admin.TelemetryGroup)
	for _, m := range metrics {
		if _, ok := groupMap[m.GroupID]; !ok {
			groupMap[m.GroupID] = &admin.TelemetryGroup{GroupID: m.GroupID}
		}

		groupMap[m.GroupID].Consumers = append(groupMap[m.GroupID].Consumers, admin.TelemetryConsumer{
			Name:    m.ConsumerName,
			Workers: m.WorkersCount,
			Assignments: []admin.TelemetryTopicAssignment{{
				Topic:             m.Topic,
				InstanceName:      m.InstanceName,
				Status:            m.Status,
				RunningPartitions: m.RunningPartitions,
				PausedPartitions:  m.PausedPartitions,
				LastUpdate:        m.SentAt,
				Lag:               m.Lag,
			}},
		})
	}

	groups := make([]admin.TelemetryGroup, 0, len(groupMap))
	for _, g := range groupMap {
		groups = append(groups, *g)
	}

	c.JSON(http.StatusOK, admin.TelemetryResponse{Groups: groups})
}
