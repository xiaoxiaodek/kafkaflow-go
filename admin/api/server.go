package api

import (
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
)

// Server is the Gin HTTP server for the admin API.
type Server struct {
	engine    *gin.Engine
	ca        *admin.ConsumerAdmin
	consumers []*consumer.ConsumerManager
	storage   admin.TelemetryStorage
}

// NewServer creates a new admin API server.
func NewServer(
	ca *admin.ConsumerAdmin,
	consumers []*consumer.ConsumerManager,
	storage admin.TelemetryStorage,
) *Server {
	r := gin.Default()

	s := &Server{
		engine:    r,
		ca:        ca,
		consumers: consumers,
		storage:   storage,
	}

	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	kf := s.engine.Group("/kafkaflow")
	{
		kf.GET("/groups", s.listGroups)
		kf.POST("/groups/:groupId/pause", s.pauseGroup)
		kf.POST("/groups/:groupId/resume", s.resumeGroup)

		kf.GET("/groups/:groupId/consumers", s.listConsumers)
		kf.GET("/groups/:groupId/consumers/:consumerName", s.getConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/pause", s.pauseConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/resume", s.resumeConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/start", s.startConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/stop", s.stopConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/restart", s.restartConsumer)
		kf.POST("/groups/:groupId/consumers/:consumerName/reset-offsets", s.resetOffsets)
		kf.POST("/groups/:groupId/consumers/:consumerName/rewind-offsets-to-date", s.rewindOffsets)
		kf.POST("/groups/:groupId/consumers/:consumerName/change-worker-count", s.changeWorkerCount)

		kf.GET("/telemetry", s.getTelemetry)
	}

	s.registerDashboardRoutes(kf)
	s.serveDashboard()
}

func (s *Server) serveDashboard() {
	distPath := s.findDashboardDist()
	if distPath == "" {
		return
	}

	s.engine.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path
		if len(path) >= 10 && path[:10] == "/kafkaflow" {
			relativePath := path[10:]
			if relativePath == "" || relativePath == "/" {
				relativePath = "/index.html"
			}
			filePath := filepath.Join(distPath, relativePath)
			if _, err := os.Stat(filePath); err == nil {
				c.File(filePath)
				return
			}
			c.File(filepath.Join(distPath, "index.html"))
			return
		}
		c.Status(http.StatusNotFound)
	})
}

func (s *Server) findDashboardDist() string {
	paths := []string{
		"dashboard/dist",
		"../dashboard/dist",
	}
	for _, p := range paths {
		absPath, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		if info, err := os.Stat(absPath); err == nil && info.IsDir() {
			return absPath
		}
	}
	return ""
}

// Run starts the HTTP server.
func (s *Server) Run(addr string) error {
	return s.engine.Run(addr)
}
