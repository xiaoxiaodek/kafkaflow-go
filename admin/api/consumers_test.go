package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPauseConsumerReturnsNotFoundWhenConsumerDoesNotExist(t *testing.T) {
	s := NewServer(nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/kafkaflow/groups/group-1/consumers/missing/pause", nil)
	w := httptest.NewRecorder()

	s.engine.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", w.Code)
	}
}
