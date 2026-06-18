package avro

import (
	"github.com/hamba/avro/v2"
)

// Serializer serializes values using an Avro schema.
type Serializer struct {
	schema avro.Schema
}

// NewSerializer creates a new Avro Serializer from a schema JSON string.
func NewSerializer(schemaJSON string) (*Serializer, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	return &Serializer{schema: schema}, nil
}

// Serialize marshals a value to Avro binary format.
func (s *Serializer) Serialize(value interface{}) ([]byte, error) {
	return avro.Marshal(s.schema, value)
}

// Deserializer deserializes Avro binary data using a schema.
type Deserializer struct {
	schema avro.Schema
}

// NewDeserializer creates a new Avro Deserializer from a schema JSON string.
func NewDeserializer(schemaJSON string) (*Deserializer, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	return &Deserializer{schema: schema}, nil
}

// Deserialize unmarshals Avro binary data into the target.
func (d *Deserializer) Deserialize(data []byte, target interface{}) error {
	return avro.Unmarshal(d.schema, data, target)
}
