package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
)

func (s *Server) listConsumers(c *gin.Context) {
	groupID := c.Param("groupId")

	var consumers []admin.ConsumerResponse
	for _, cm := range s.consumers {
		if cm.GroupID() == groupID {
			consumers = append(consumers, admin.ConsumerResponse{
				Name:    cm.Name(),
				GroupID: cm.GroupID(),
				Topics:  cm.Topics(),
				Workers: cm.WorkerCount(),
				Status:  cm.Status(),
			})
		}
	}

	c.JSON(http.StatusOK, admin.ConsumersResponse{Consumers: consumers})
}

func (s *Server) getConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")

	for _, cm := range s.consumers {
		if cm.GroupID() == groupID && cm.Name() == consumerName {
			c.JSON(http.StatusOK, admin.ConsumerResponse{
				Name:    cm.Name(),
				GroupID: cm.GroupID(),
				Topics:  cm.Topics(),
				Workers: cm.WorkerCount(),
				Status:  cm.Status(),
			})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
}

func (s *Server) pauseConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")
	topics := c.QueryArray("topics")

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.PauseConsumer(c.Request.Context(), consumerName, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) resumeConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")
	topics := c.QueryArray("topics")

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.ResumeConsumer(c.Request.Context(), consumerName, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) startConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.StartConsumer(c.Request.Context(), consumerName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) stopConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.StopConsumer(c.Request.Context(), consumerName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) restartConsumer(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.RestartConsumer(c.Request.Context(), consumerName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) resetOffsets(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")
	topics := c.QueryArray("topics")

	var req admin.ResetOffsetsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !req.Confirm {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirm must be true to reset offsets"})
		return
	}

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.ResetConsumerOffset(c.Request.Context(), consumerName, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) rewindOffsets(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")
	topics := c.QueryArray("topics")

	var req admin.RewindOffsetsToDateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.RewindConsumerOffset(c.Request.Context(), consumerName, req.Timestamp, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) changeWorkerCount(c *gin.Context) {
	groupID := c.Param("groupId")
	consumerName := c.Param("consumerName")

	var req admin.ChangeWorkersCountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !s.consumerExists(groupID, consumerName) {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	if err := s.ca.ChangeWorkersCount(c.Request.Context(), consumerName, req.WorkersCount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) consumerExists(groupID string, consumerName string) bool {
	for _, cm := range s.consumers {
		if cm.GroupID() == groupID && cm.Name() == consumerName {
			return true
		}
	}
	return false
}
