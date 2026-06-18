package schemaregistry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	sr "github.com/xiaoxiaodek/kafkaflow-go/serializer/schemaregistry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufDeserializer_ShortData(t *testing.T) {
	d := sr.NewProtobufDeserializer(nil, func() proto.Message { return &wrapperspb.StringValue{} })
	err := d.Deserialize([]byte{0x00}, nil)
	assert.Error(t, err)
}

func TestProtobufDeserializer_BadMagicByte(t *testing.T) {
	d := sr.NewProtobufDeserializer(nil, func() proto.Message { return &wrapperspb.StringValue{} })
	err := d.Deserialize([]byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00}, nil)
	assert.Error(t, err)
}

func TestProtobufDeserializer_ValidWireFormat(t *testing.T) {
	d := sr.NewProtobufDeserializer(nil, func() proto.Message { return &wrapperspb.StringValue{} })
	err := d.Deserialize([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, nil)
	assert.NoError(t, err)
}
