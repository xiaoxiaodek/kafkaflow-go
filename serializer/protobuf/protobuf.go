package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Serializer serializes protobuf messages.
type Serializer struct{}

// NewSerializer creates a new Protobuf Serializer.
func NewSerializer() *Serializer { return &Serializer{} }

// Serialize marshals a proto.Message to bytes.
func (s *Serializer) Serialize(value interface{}) ([]byte, error) {
	msg, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf: value must implement proto.Message, got %T", value)
	}
	return proto.Marshal(msg)
}

// Deserializer deserializes protobuf messages.
type Deserializer struct {
	factory func() proto.Message
}

// NewDeserializer creates a new Protobuf Deserializer with a factory function
// that creates new instances of the target message type.
func NewDeserializer(factory func() proto.Message) *Deserializer {
	return &Deserializer{factory: factory}
}

// Deserialize unmarshals protobuf bytes into a new message instance.
// The target parameter is ignored; the result is created via the factory.
func (d *Deserializer) Deserialize(data []byte, target interface{}) error {
	msg := d.factory()
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}
	return nil
}
