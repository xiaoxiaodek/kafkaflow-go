package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Compressor compresses data using gzip.
type Compressor struct{}

// NewCompressor creates a new Gzip Compressor.
func NewCompressor() *Compressor {
	return &Compressor{}
}

// Compress compresses data using gzip.
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompressor decompresses gzip-compressed data.
type Decompressor struct{}

// NewDecompressor creates a new Gzip Decompressor.
func NewDecompressor() *Decompressor {
	return &Decompressor{}
}

// Decompress decompresses gzip-compressed data.
func (d *Decompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
