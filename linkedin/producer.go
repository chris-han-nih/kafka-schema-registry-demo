package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

func main() {
	// Define Avro schema as a single definition
	schemaDefinition := `
{
	"namespace": "my.namespace.com",
	"type":	"record",
	"name": "identity",
	"fields": [
		{ "name": "FirstName", "type": "string"},
		{ "name": "LastName", "type": "string"},
		{ "name": "Errors", "type": ["null", {"type":"array", "items":"string"}], "default": null },
		{ "name": "Address", "type": ["null",{
			"type":	"record",
			"name": "address",
			"fields": [
				{ "name": "Address1", "type": "string" },
				{ "name": "City", "type": "string" },
				{ "name": "State", "type": "string" },
				{ "name": "Zip", "type": "int" }
			]
		}],"default":null}
	]
}`

	// Create Schema Registry client
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schema, err := schemaRegistryClient.CreateSchema("test_topic-value-1", schemaDefinition, srclient.Avro)
	if err != nil {
		panic(err)
	}

	// Create Avro binary from native Go form
	binary, err := schema.Codec().BinaryFromNative(nil, map[string]interface{}{
		"FirstName": "John",
		"LastName":  "Doe",
		"Errors":    nil,
		"Address": map[string]interface{}{
			"my.namespace.com.address": map[string]interface{}{
				"Address1": "eunjuro",
				"City":     "Springfield",
				"State":    "IL",
				"Zip":      12345,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// Setup Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	// Produce message
	topic := "test_topic"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          binary,
	}, nil)

	p.Flush(15 * 1000)
}
