package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

func main() {
	// Setup Kafka consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup16",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics([]string{"test_topic"}, nil)
	if err != nil {
		return
	}

	// Create Schema Registry client
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	for {
		msg, _ := c.ReadMessage(-1)
		if err == nil {
			schema, err := schemaRegistryClient.GetLatestSchema("test_topic-value-1")
			if err != nil {
				panic(err)
			}
			codec, err := goavro.NewCodec(schema.Schema())
			if err != nil {
				panic(err)
			}
			native, _, err := codec.NativeFromBinary(msg.Value)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Received message: %v\n", native)
			fmt.Printf("Received message: %v\n", native.(map[string]interface{}))
			//textural, err := codec.TextualFromNative(nil, native.(map[string]interface{}))
			//if err != nil {
			//	panic(err)
			//}
			u := StringMapToUser(native.(map[string]interface{}))
			fmt.Printf("Received message: %v\n", u)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

type User struct {
	FirstName string   `avro:"FirstName"`
	LastName  string   `avro:"LastName"`
	Errors    []string `avro:"Errors"`
	Address   Address  `avro:"Address"`
}

type Address struct {
	Address1 string `avro:"Address1"`
	City     string `avro:"City"`
	State    string `avro:"State"`
	Zip      int    `avro:"Zip"`
}

func StringMapToUser(data map[string]interface{}) *User {

	ind := &User{}
	for k, v := range data {
		switch k {
		case "FirstName":
			if value, ok := v.(string); ok {
				ind.FirstName = value
			}
		case "LastName":
			if value, ok := v.(string); ok {
				ind.LastName = value
			}
		case "Errors":
			if value, ok := v.(map[string]interface{}); ok {
				for _, item := range value["array"].([]interface{}) {
					ind.Errors = append(ind.Errors, item.(string))
				}
			}
		case "Address":
			if vmap, ok := v.(map[string]interface{}); ok {
				//important need namespace and record name
				if cookieSMap, ok := vmap["my.namespace.com.address"].(map[string]interface{}); ok {
					add := &Address{}
					for k, v := range cookieSMap {
						switch k {
						case "Address1":
							if value, ok := v.(string); ok {
								add.Address1 = value
							}
						case "City":
							if value, ok := v.(string); ok {
								add.City = value
							}
						case "State":
							if value, ok := v.(string); ok {
								add.State = value
							}
						case "Zip":
							if value, ok := v.(int); ok {
								add.Zip = value
							}
						}
					}
					ind.Address = *add
				}
			}
		}
	}
	return ind
}
