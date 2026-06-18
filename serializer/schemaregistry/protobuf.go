package schemaregistry

import (
	"encoding/binary"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"google.golang.org/protobuf/proto"
)

// ProtobufSerializer serializes using Schema Registry with Confluent wire format.
// Confluent Protobuf wire format: [0x00 magic byte] [4-byte schema ID big-endian] [message indexes] [protobuf data]
type ProtobufSerializer struct {
	client   schemaregistry.Client
	subject  string
	schemaID int
}

// NewProtobufSerializer creates a new Schema Registry Protobuf serializer.
func NewProtobufSerializer(client schemaregistry.Client, subject string, schemaJSON string) (*ProtobufSerializer, error) {
	si := schemaregistry.SchemaInfo{
		Schema:     schemaJSON,
		SchemaType: "PROTOBUF",
	}

	id, err := client.Register(subject, si, true)
	if err != nil {
		id, err = client.GetID(subject, si, true)
		if err != nil {
			return nil, fmt.Errorf("protobuf schema registry register/lookup error: %w", err)
		}
	}

	return &ProtobufSerializer{
		client:   client,
		subject:  subject,
		schemaID: id,
	}, nil
}

// Serialize encodes a proto.Message using Confluent wire format.
func (s *ProtobufSerializer) Serialize(value interface{}) ([]byte, error) {
	msg, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf: value must implement proto.Message, got %T", value)
	}

	pbData, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("protobuf marshal error: %w", err)
	}

	buf := make([]byte, 6+len(pbData))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(s.schemaID))
	buf[5] = 0x00
	copy(buf[6:], pbData)

	return buf, nil
}

// ProtobufDeserializer deserializes Schema Registry Protobuf messages with full schema ID validation.
type ProtobufDeserializer struct {
	client  schemaregistry.Client
	factory func() proto.Message
}

// NewProtobufDeserializer creates a new Schema Registry Protobuf deserializer.
func NewProtobufDeserializer(client schemaregistry.Client, factory func() proto.Message) *ProtobufDeserializer {
	return &ProtobufDeserializer{client: client, factory: factory}
}

// Deserialize decodes Confluent wire format, validates schema ID against registry, and deserializes protobuf data.
func (d *ProtobufDeserializer) Deserialize(data []byte, target interface{}) error {
	if len(data) < 6 {
		return fmt.Errorf("protobuf: data too short for wire format (need at least 6 bytes, got %d)", len(data))
	}
	if data[0] != 0x00 {
		return fmt.Errorf("protobuf: invalid magic byte 0x%02x, expected 0x00", data[0])
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	pbData := data[6:]

	if d.client != nil {
		if _, err := d.client.GetBySubjectAndID("", schemaID); err != nil {
			return fmt.Errorf("protobuf: schema ID %d not found in registry: %w", schemaID, err)
		}
	}

	msg := d.factory()
	if err := proto.Unmarshal(pbData, msg); err != nil {
		return fmt.Errorf("protobuf unmarshal error: %w", err)
	}

	return nil
}
