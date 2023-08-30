package main

//func main() {
//	// Setup Kafka consumer
//	c, err := kafka.NewConsumer(&kafka.ConfigMap{
//		"bootstrap.servers": "localhost:9092",
//		"group.id":          "myGroup",
//		"auto.offset.reset": "earliest",
//	})
//	if err != nil {
//		panic(err)
//	}
//
//	c.SubscribeTopics([]string{"test_topic"}, nil)
//
//	// Create Schema Registry client
//	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
//
//	for {
//		msg, err := c.ReadMessage(-1)
//		if err == nil {
//			schema, err := schemaRegistryClient.GetLatestSchema("test_topic-value")
//			if err != nil {
//				panic(err)
//			}
//			codec, err := goavro.NewCodec(schema.Schema())
//			if err != nil {
//				panic(err)
//			}
//			native, _, err := codec.NativeFromBinary(msg.Value)
//			if err != nil {
//				panic(err)
//			}
//			fmt.Printf("Received message: %v\n", native)
//		} else {
//			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
//		}
//	}
//}
