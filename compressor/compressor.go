package compressor

// Compressor compresses a byte slice.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
}

// Decompressor decompresses a byte slice.
type Decompressor interface {
	Decompress(data []byte) ([]byte, error)
}
