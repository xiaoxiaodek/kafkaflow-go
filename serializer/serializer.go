package serializer

// Serializer converts a value to bytes for Kafka message values.
type Serializer interface {
	Serialize(value interface{}) ([]byte, error)
}

// Deserializer converts bytes back to a value.
type Deserializer interface {
	Deserialize(data []byte, target interface{}) error
}
