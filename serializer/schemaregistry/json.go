package schemaregistry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	srclient "github.com/xiaoxiaodek/kafkaflow-go/schemaregistry"
)

// JSONSerializer serializes JSON values using Schema Registry with Confluent wire format.
// Confluent JSON Schema wire format: [0x00 magic byte] [4-byte schema ID big-endian] [JSON payload]
type JSONSerializer struct {
	client   *srclient.Client
	subject  string
	schemaID int
}

// NewJSONSerializer creates a new Schema Registry JSON serializer.
// It registers or looks up the JSON schema and caches the schema ID.
func NewJSONSerializer(client *srclient.Client, subject string, schemaJSON string) (*JSONSerializer, error) {
	schema := confluent.SchemaInfo{Schema: schemaJSON, SchemaType: "JSON"}
	id, err := client.Register(subject, schema)
	if err != nil {
		id, err = client.LookupSchema(subject, schema)
		if err != nil {
			return nil, fmt.Errorf("json schema registry register/lookup error: %w", err)
		}
	}
	return &JSONSerializer{client: client, subject: subject, schemaID: id}, nil
}

// Serialize encodes a value using Confluent JSON Schema wire format.
func (s *JSONSerializer) Serialize(value interface{}) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("json schema registry: marshal error: %w", err)
	}
	return encodeJSONWireFormat(s.schemaID, payload), nil
}

// JSONDeserializer deserializes Schema Registry JSON messages with full Confluent wire format support.
type JSONDeserializer struct {
	client *srclient.Client
}

// NewJSONDeserializer creates a new Schema Registry JSON deserializer.
func NewJSONDeserializer(client *srclient.Client) *JSONDeserializer {
	return &JSONDeserializer{client: client}
}

// Deserialize decodes Confluent wire format, validates schema ID against registry, and unmarshals JSON.
func (d *JSONDeserializer) Deserialize(data []byte, target interface{}) error {
	if len(data) < 5 {
		return fmt.Errorf("json schema registry: data too short for wire format (need at least 5 bytes, got %d)", len(data))
	}
	if data[0] != 0x00 {
		return fmt.Errorf("json schema registry: invalid magic byte 0x%02x, expected 0x00", data[0])
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	jsonPayload := data[5:]

	if d.client != nil {
		if _, err := d.client.GetSchemaBySubjectAndID("", schemaID); err != nil {
			return fmt.Errorf("json schema registry: schema ID %d not found in registry: %w", schemaID, err)
		}
	}

	return json.Unmarshal(jsonPayload, target)
}

func encodeJSONWireFormat(schemaID int, payload []byte) []byte {
	buf := make([]byte, 5+len(payload))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], payload)
	return buf
}

func schemaIDFromWireFormat(data []byte) int {
	if len(data) < 5 {
		return 0
	}
	return int(binary.BigEndian.Uint32(data[1:5]))
}
