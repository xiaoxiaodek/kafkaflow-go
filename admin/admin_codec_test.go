package admin

import (
	"testing"

	"github.com/xiaoxiaodek/kafkaflow-go/admin/adminpb"
	"google.golang.org/protobuf/proto"
)

func TestEncodeDecodePauseConsumerByNameUsesProtobufPayload(t *testing.T) {
	payload, err := encodeAdminMessage(&PauseConsumerByName{ConsumerName: "orders", Topics: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("encode admin message: %v", err)
	}
	if len(payload) == 0 {
		t.Fatalf("expected non-empty protobuf payload")
	}
	if payload[0] == '{' {
		t.Fatalf("expected protobuf payload, got JSON %q", string(payload))
	}

	decoded, err := decodeAdminMessage("PauseConsumerByName", payload)
	if err != nil {
		t.Fatalf("decode admin message: %v", err)
	}
	msg, ok := decoded.(*PauseConsumerByName)
	if !ok {
		t.Fatalf("expected *PauseConsumerByName, got %T", decoded)
	}
	if msg.ConsumerName != "orders" || len(msg.Topics) != 2 || msg.Topics[0] != "a" || msg.Topics[1] != "b" {
		t.Fatalf("decoded message mismatch: %#v", msg)
	}
}

func TestEncodeDecodeAllMessageTypes(t *testing.T) {
	tests := []struct {
		name    string
		msg     AdminMessage
		msgType string
	}{
		{"StartConsumerByName", &StartConsumerByName{ConsumerName: "c1"}, "StartConsumerByName"},
		{"StopConsumerByName", &StopConsumerByName{ConsumerName: "c1"}, "StopConsumerByName"},
		{"RestartConsumerByName", &RestartConsumerByName{ConsumerName: "c1"}, "RestartConsumerByName"},
		{"ResetConsumerOffset", &ResetConsumerOffset{ConsumerName: "c1", Topics: []string{"t1"}}, "ResetConsumerOffset"},
		{"RewindConsumerOffsetToDateTime", &RewindConsumerOffsetToDateTime{ConsumerName: "c1", Topics: []string{"t1"}, Timestamp: 12345}, "RewindConsumerOffsetToDateTime"},
		{"ChangeConsumerWorkersCount", &ChangeConsumerWorkersCount{ConsumerName: "c1", WorkersCount: 5}, "ChangeConsumerWorkersCount"},
		{"PauseConsumersByGroup", &PauseConsumersByGroup{GroupID: "g1", Topics: []string{"t1"}}, "PauseConsumersByGroup"},
		{"ResumeConsumersByGroup", &ResumeConsumersByGroup{GroupID: "g1", Topics: []string{"t1"}}, "ResumeConsumersByGroup"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := encodeAdminMessage(tt.msg)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			decoded, err := decodeAdminMessage(tt.msgType, payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if decoded == nil {
				t.Fatal("decoded message is nil")
			}
		})
	}
}

func TestEncodeDecodeUnknownType(t *testing.T) {
	_, err := decodeAdminMessage("UnknownType", []byte("garbage"))
	if err == nil {
		t.Fatal("expected error for garbage data")
	}
}

func TestAdminCommandRoundTrip(t *testing.T) {
	original := &PauseConsumerByName{ConsumerName: "test-consumer", Topics: []string{"topic1", "topic2"}}
	data, err := encodeAdminMessage(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeAdminMessage("PauseConsumerByName", data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	msg, ok := decoded.(*PauseConsumerByName)
	if !ok {
		t.Fatalf("expected *PauseConsumerByName, got %T", decoded)
	}

	if msg.ConsumerName != original.ConsumerName {
		t.Fatalf("consumer name mismatch: got %q, want %q", msg.ConsumerName, original.ConsumerName)
	}
	if len(msg.Topics) != len(original.Topics) {
		t.Fatalf("topics length mismatch: got %d, want %d", len(msg.Topics), len(original.Topics))
	}
	for i, topic := range msg.Topics {
		if topic != original.Topics[i] {
			t.Fatalf("topic[%d] mismatch: got %q, want %q", i, topic, original.Topics[i])
		}
	}
}

func TestProtobufBinaryFormat(t *testing.T) {
	data, err := encodeAdminMessage(&StartConsumerByName{ConsumerName: "my-consumer"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var cmd adminpb.AdminCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	startCmd, ok := cmd.Command.(*adminpb.AdminCommand_StartConsumerByName)
	if !ok {
		t.Fatalf("expected StartConsumerByName, got %T", cmd.Command)
	}
	if startCmd.StartConsumerByName.ConsumerName != "my-consumer" {
		t.Fatalf("expected 'my-consumer', got %q", startCmd.StartConsumerByName.ConsumerName)
	}
}
