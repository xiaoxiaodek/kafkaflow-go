package middleware_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
)

type testCompressor struct{}

func (c *testCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(data)
	w.Close()
	return buf.Bytes(), nil
}

type testDecompressor struct{}

func (d *testDecompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func TestCompressorMiddleware(t *testing.T) {
	c := &testCompressor{}
	mw := middleware.Compressor(c)

	var captured []byte
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		captured = mc.Message.Value.([]byte)
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	original := []byte("hello world")
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: original},
	}

	err := pipeline(context.Background(), mc)
	require.NoError(t, err)
	assert.NotEqual(t, original, captured, "compressed data should differ from original")
}

func TestDecompressorMiddleware(t *testing.T) {
	c := &testCompressor{}
	d := &testDecompressor{}

	original := []byte("hello world")

	compressorMw := middleware.Compressor(c)
	decompressorMw := middleware.Decompressor(d)

	var captured []byte
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		captured = mc.Message.Value.([]byte)
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{compressorMw, decompressorMw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: original},
	}

	err := pipeline(context.Background(), mc)
	require.NoError(t, err)
	assert.Equal(t, original, captured)
}
