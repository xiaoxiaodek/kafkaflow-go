package admin

import "testing"

func TestBuildAdminConsumerAssignmentUsesFixedPartition(t *testing.T) {
	assignment := buildAdminConsumerAssignment("admin-topic", 0)
	if len(assignment) != 1 {
		t.Fatalf("expected 1 partition assignment, got %d", len(assignment))
	}
	if assignment[0].Topic == nil || *assignment[0].Topic != "admin-topic" {
		t.Fatalf("expected admin-topic, got %v", assignment[0].Topic)
	}
	if assignment[0].Partition != 0 {
		t.Fatalf("expected partition 0, got %d", assignment[0].Partition)
	}
}
