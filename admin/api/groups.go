package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
)

func (s *Server) listGroups(c *gin.Context) {
	groupMap := make(map[string]*admin.GroupResponse)

	for _, cm := range s.consumers {
		gid := cm.GroupID()
		if _, ok := groupMap[gid]; !ok {
			groupMap[gid] = &admin.GroupResponse{GroupID: gid}
		}
		groupMap[gid].Consumers = append(groupMap[gid].Consumers, admin.ConsumerResponse{
			Name:    cm.Name(),
			GroupID: cm.GroupID(),
			Topics:  cm.Topics(),
			Workers: cm.WorkerCount(),
			Status:  cm.Status(),
		})
	}

	groups := make([]admin.GroupResponse, 0, len(groupMap))
	for _, g := range groupMap {
		groups = append(groups, *g)
	}

	c.JSON(http.StatusOK, admin.GroupsResponse{Groups: groups})
}

func (s *Server) pauseGroup(c *gin.Context) {
	groupID := c.Param("groupId")
	topics := c.QueryArray("topics")

	if err := s.ca.PauseGroup(c.Request.Context(), groupID, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) resumeGroup(c *gin.Context) {
	groupID := c.Param("groupId")
	topics := c.QueryArray("topics")

	if err := s.ca.ResumeGroup(c.Request.Context(), groupID, topics); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusAccepted)
}
