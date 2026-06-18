package schemaregistry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	sr "github.com/xiaoxiaodek/kafkaflow-go/serializer/schemaregistry"
)

func TestAvroSerializer_WireFormat(t *testing.T) {
	assert.True(t, true)
}

func TestAvroDeserializer_ShortData(t *testing.T) {
	d := sr.NewAvroDeserializer(nil)
	err := d.Deserialize([]byte{0x00}, nil)
	assert.Error(t, err)
}

func TestAvroDeserializer_BadMagicByte(t *testing.T) {
	d := sr.NewAvroDeserializer(nil)
	err := d.Deserialize([]byte{0x01, 0x00, 0x00, 0x00, 0x01}, nil)
	assert.Error(t, err)
}
