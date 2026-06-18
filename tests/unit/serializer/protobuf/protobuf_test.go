package protobuf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/xiaoxiaodek/kafkaflow-go/serializer/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufSerializer_Serialize(t *testing.T) {
	s := pb.NewSerializer()
	msg := wrapperspb.String("hello")
	data, err := s.Serialize(msg)
	require.NoError(t, err)
	assert.NotEmpty(t, data)
}

func TestProtobufSerializer_SerializeNonProto(t *testing.T) {
	s := pb.NewSerializer()
	_, err := s.Serialize("not a proto message")
	assert.Error(t, err)
}

func TestProtobufDeserializer_Deserialize(t *testing.T) {
	s := pb.NewSerializer()
	d := pb.NewDeserializer(func() proto.Message { return &wrapperspb.StringValue{} })

	original := wrapperspb.String("hello")
	data, err := s.Serialize(original)
	require.NoError(t, err)

	var result interface{}
	err = d.Deserialize(data, &result)
	require.NoError(t, err)
}
