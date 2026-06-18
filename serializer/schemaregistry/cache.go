package schemaregistry

import (
	"sync"

	"github.com/hamba/avro/v2"
)

type schemaCache struct {
	mu      sync.RWMutex
	schemas map[int]avro.Schema
}

var globalSchemaCache = &schemaCache{
	schemas: make(map[int]avro.Schema),
}

func (c *schemaCache) get(schemaID int) (avro.Schema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.schemas[schemaID]
	return s, ok
}

func (c *schemaCache) set(schemaID int, schema avro.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[schemaID] = schema
}
