package schemaregistry

import "testing"

func TestJSONDeserializerRejectsShortWireFormat(t *testing.T) {
	d := NewJSONDeserializer(nil)
	var target map[string]interface{}
	if err := d.Deserialize([]byte{0x00}, &target); err == nil {
		t.Fatal("expected error for short wire format")
	}
}

func TestJSONWireFormatEncodesMagicByteAndSchemaID(t *testing.T) {
	data := encodeJSONWireFormat(42, []byte(`{"id":"1"}`))
	if len(data) < 5 {
		t.Fatalf("expected wire data length >= 5, got %d", len(data))
	}
	if data[0] != 0x00 {
		t.Fatalf("expected magic byte 0, got %d", data[0])
	}
	if got := schemaIDFromWireFormat(data); got != 42 {
		t.Fatalf("expected schema ID 42, got %d", got)
	}
}
