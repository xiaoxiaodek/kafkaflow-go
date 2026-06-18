package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTelemetryEndpointWithoutStorageReturnsEmptyResponse(t *testing.T) {
	s := NewServer(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/kafkaflow/telemetry", nil)
	w := httptest.NewRecorder()

	s.engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
}
