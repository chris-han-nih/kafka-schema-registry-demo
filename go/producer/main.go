package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
	"nih/model"
	"os"
	"path/filepath"
)

const (
	SchemaRegistryURL = "http://localhost:8081"
)

func main() {
	Topic := "test"

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(SchemaRegistryURL)

	filePath, _ := filepath.Abs("producer/avro/agent.avsc")

	agentSchema, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	schemaOfAgent, err := schemaRegistryClient.CreateSchema("agent", string(agentSchema), srclient.Avro)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:29092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// "Agent" record to send
	agentRecord := model.Agent{
		Id:    1,
		Email: "dave.han@nsuslab.com",
		Group: "admin",
	}
	value, _ := json.Marshal(agentRecord)
	native, _, _ := schemaOfAgent.Codec().NativeFromTextual(value)
	binary, err := schemaOfAgent.Codec().BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &Topic, Partition: kafka.PartitionAny},
		Value:          binary,
	}

	err = p.Produce(msg, nil)
	if err != nil {
		panic(err)
	}
}
