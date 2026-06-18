package json

import (
	"encoding/json"
)

// Serializer is a JSON serializer using encoding/json.
type Serializer struct{}

// NewSerializer creates a new JSON Serializer.
func NewSerializer() *Serializer {
	return &Serializer{}
}

// Serialize marshals a value to JSON bytes.
func (s *Serializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Deserializer is a JSON deserializer using encoding/json.
type Deserializer struct{}

// NewDeserializer creates a new JSON Deserializer.
func NewDeserializer() *Deserializer {
	return &Deserializer{}
}

// Deserialize unmarshals JSON bytes into the target.
func (d *Deserializer) Deserialize(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}
