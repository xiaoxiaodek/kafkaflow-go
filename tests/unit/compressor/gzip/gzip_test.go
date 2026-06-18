package gzip_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gzipcompressor "github.com/xiaoxiaodek/kafkaflow-go/compressor/gzip"
)

func TestCompressor_CompressDecompress_RoundTrip(t *testing.T) {
	c := gzipcompressor.NewCompressor()
	d := gzipcompressor.NewDecompressor()

	original := []byte("hello world, this is a test message for gzip compression")
	compressed, err := c.Compress(original)
	require.NoError(t, err)
	assert.NotEqual(t, original, compressed, "compressed data should differ from original")

	decompressed, err := d.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressor_CompressEmpty(t *testing.T) {
	c := gzipcompressor.NewCompressor()
	d := gzipcompressor.NewDecompressor()

	compressed, err := c.Compress([]byte{})
	require.NoError(t, err)

	decompressed, err := d.Decompress(compressed)
	require.NoError(t, err)
	assert.Equal(t, []byte{}, decompressed)
}

func TestDecompressor_DecompressInvalid(t *testing.T) {
	d := gzipcompressor.NewDecompressor()
	_, err := d.Decompress([]byte("not gzip data"))
	assert.Error(t, err)
}
