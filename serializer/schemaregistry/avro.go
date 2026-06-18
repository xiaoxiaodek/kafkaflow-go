package schemaregistry

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	srclient "github.com/xiaoxiaodek/kafkaflow-go/schemaregistry"
)

// AvroSerializer serializes using Schema Registry with Confluent wire format.
// Wire format: [0x00] [4-byte schema ID] [avro binary data]
type AvroSerializer struct {
	client   *srclient.Client
	subject  string
	schemaID int
	schema   avro.Schema
}

// NewAvroSerializer creates a new Schema Registry Avro serializer.
// It registers or looks up the schema and caches the schema ID.
func NewAvroSerializer(client *srclient.Client, subject string, schemaJSON string) (*AvroSerializer, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("avro schema parse error: %w", err)
	}

	si := schemaregistry.SchemaInfo{
		Schema:     schemaJSON,
		SchemaType: "AVRO",
	}

	id, err := client.Register(subject, si)
	if err != nil {
		id, err = client.LookupSchema(subject, si)
		if err != nil {
			return nil, fmt.Errorf("schema registry register/lookup error: %w", err)
		}
	}

	return &AvroSerializer{
		client:   client,
		subject:  subject,
		schemaID: id,
		schema:   schema,
	}, nil
}

// Serialize encodes a value using Confluent wire format.
func (s *AvroSerializer) Serialize(value interface{}) ([]byte, error) {
	avroData, err := avro.Marshal(s.schema, value)
	if err != nil {
		return nil, fmt.Errorf("avro marshal error: %w", err)
	}

	buf := make([]byte, 5+len(avroData))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(s.schemaID))
	copy(buf[5:], avroData)

	return buf, nil
}

// AvroDeserializer deserializes Schema Registry Avro messages.
type AvroDeserializer struct {
	client *srclient.Client
}

// NewAvroDeserializer creates a new Schema Registry Avro deserializer.
func NewAvroDeserializer(client *srclient.Client) *AvroDeserializer {
	return &AvroDeserializer{client: client}
}

// Deserialize decodes Confluent wire format and deserializes Avro data.
func (d *AvroDeserializer) Deserialize(data []byte, target interface{}) error {
	if len(data) < 5 {
		return fmt.Errorf("avro: data too short for wire format")
	}
	if data[0] != 0x00 {
		return fmt.Errorf("avro: invalid magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	avroData := data[5:]

	schema, ok := globalSchemaCache.get(schemaID)
	if !ok {
		si, err := d.client.GetSchemaBySubjectAndID("", schemaID)
		if err != nil {
			return fmt.Errorf("avro: failed to get schema for ID %d: %w", schemaID, err)
		}

		schema, err = avro.Parse(si.Schema)
		if err != nil {
			return fmt.Errorf("avro: failed to parse schema: %w", err)
		}
		globalSchemaCache.set(schemaID, schema)
	}

	return avro.Unmarshal(schema, avroData, target)
}
