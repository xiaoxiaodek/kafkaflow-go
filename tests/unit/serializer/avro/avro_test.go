package avro_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	avroserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/avro"
)

const testSchema = `{
	"type": "record",
	"name": "test",
	"fields": [
		{"name": "name", "type": "string"},
		{"name": "value", "type": "int"}
	]
}`

type testRecord struct {
	Name  string `avro:"name"`
	Value int    `avro:"value"`
}

func TestAvroSerializer_RoundTrip(t *testing.T) {
	s, err := avroserializer.NewSerializer(testSchema)
	require.NoError(t, err)

	d, err := avroserializer.NewDeserializer(testSchema)
	require.NoError(t, err)

	original := testRecord{Name: "hello", Value: 42}
	data, err := s.Serialize(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	var result testRecord
	err = d.Deserialize(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result.Name)
	assert.Equal(t, 42, result.Value)
}

func TestAvroSerializer_InvalidSchema(t *testing.T) {
	_, err := avroserializer.NewSerializer("{invalid}")
	assert.Error(t, err)
}

func TestAvroDeserializer_InvalidData(t *testing.T) {
	d, err := avroserializer.NewDeserializer(testSchema)
	require.NoError(t, err)

	var result testRecord
	err = d.Deserialize([]byte("not avro data"), &result)
	assert.Error(t, err)
}
