package json_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
)

type testStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestSerializer_Serialize(t *testing.T) {
	s := jsonserializer.NewSerializer()
	data, err := s.Serialize(testStruct{Name: "test", Value: 42})

	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"test","value":42}`, string(data))
}

func TestSerializer_SerializeNil(t *testing.T) {
	s := jsonserializer.NewSerializer()
	data, err := s.Serialize(nil)

	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestDeserializer_Deserialize(t *testing.T) {
	d := jsonserializer.NewDeserializer()
	var result testStruct
	err := d.Deserialize([]byte(`{"name":"test","value":42}`), &result)

	require.NoError(t, err)
	assert.Equal(t, "test", result.Name)
	assert.Equal(t, 42, result.Value)
}

func TestDeserializer_DeserializeInvalidJSON(t *testing.T) {
	d := jsonserializer.NewDeserializer()
	var result testStruct
	err := d.Deserialize([]byte(`invalid`), &result)

	assert.Error(t, err)
}
