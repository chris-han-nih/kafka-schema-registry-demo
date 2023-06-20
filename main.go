package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

const (
	SchemaRegistryURL = "http://localhost:8081"
)

func main() {
	Topic := "test" // Replace with your topic name

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(SchemaRegistryURL)
	productSchema, err := schemaRegistryClient.CreateSchema("product", `
	{
	  "definitions" : {
		"record:product" : {
		  "type" : "object",
		  "required" : [ "name", "calories" ],
		  "additionalProperties" : false,
		  "properties" : {
			"name" : {"type" : "string"},
			"calories" : {"type" : "number"},
			"colour" : {"type" : "string"}
		  }
		}
	  },
	  "$ref" : "#/definitions/record:product"
	}
	`, srclient.Json)
	if err != nil {
		fmt.Println(err)
		return
	}

	userSchema, err := schemaRegistryClient.CreateSchema("user", `{"type":"record","name":"User","fields":[{"name":"id","type":"int", "description": "user의 고유번호"},{"name":"email","type":"string"},{"name":"age","type":"int","default":1}]}`, srclient.Avro)
	if err != nil {
		fmt.Println(err)
		return
	}
	agentSchema, err := schemaRegistryClient.CreateSchema("agent", `{"type":"record","name":"Agent","fields":[{"name":"id","type":"int"},{"name":"email","type":"string"},{"name":"group","type":"string"}]}`, srclient.Avro)
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

	// "Product" record to send
	productRecord := map[string]interface{}{
		"name":     "Macbook",
		"calories": "colories",
		"colour":   "colour",
	}
	// TODO: Avro codec을 사용해서 Error 발생
	binary, err := productSchema.Codec().BinaryFromNative(nil, productRecord)
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

	// 'User' record to send
	userRecord := map[string]interface{}{
		"id":    1,
		"email": "user@gmail.com",
		"age":   30,
	}
	binary, err = userSchema.Codec().BinaryFromNative(nil, userRecord)
	if err != nil {
		fmt.Println(err)
		return
	}

	msg = &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &Topic, Partition: kafka.PartitionAny},
		Value:          binary,
	}

	err = p.Produce(msg, nil)
	if err != nil {
		panic(err)
	}

	// "Agent" record to send
	agentRecord := map[string]interface{}{
		"id":    1,
		"email": "agent@gmail.com",
		"group": "group1",
	}
	binary, err = agentSchema.Codec().BinaryFromNative(nil, agentRecord)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg = &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &Topic, Partition: kafka.PartitionAny},
		Value:          binary,
	}

	err = p.Produce(msg, nil)
	if err != nil {
		panic(err)
	}
}
