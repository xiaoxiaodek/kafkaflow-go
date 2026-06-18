package schemaregistry

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// Client wraps the Confluent Schema Registry client.
type Client struct {
	client schemaregistry.Client
}

// NewClient creates a new Schema Registry client.
func NewClient(url string) (*Client, error) {
	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return nil, err
	}
	return &Client{client: c}, nil
}

// GetLatestSchemaMetadata returns the latest schema metadata for a subject.
func (c *Client) GetLatestSchemaMetadata(subject string) (schemaregistry.SchemaMetadata, error) {
	return c.client.GetLatestSchemaMetadata(subject)
}

// GetSchemaBySubjectAndID returns the schema info for a given subject and version ID.
func (c *Client) GetSchemaBySubjectAndID(subject string, id int) (schemaregistry.SchemaInfo, error) {
	return c.client.GetBySubjectAndID(subject, id)
}

// Register registers a new schema under a subject and returns the schema ID.
func (c *Client) Register(subject string, schema schemaregistry.SchemaInfo) (int, error) {
	return c.client.Register(subject, schema, false)
}

// LookupSchema finds the schema ID for a given schema under a subject.
func (c *Client) LookupSchema(subject string, schema schemaregistry.SchemaInfo) (int, error) {
	return c.client.GetID(subject, schema, false)
}
