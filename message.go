package kafkaflow

// Header represents a Kafka message header.
type Header struct {
	Key   string
	Value []byte
}

// Message represents a Kafka message with key, value, headers, and metadata.
type Message struct {
	Key       []byte
	Value     interface{}
	Headers   []Header
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}
