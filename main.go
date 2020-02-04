package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
)

type customer struct {
	Name   string  `json:"name"`
	Age    int     `json:"age"`
	Height float32 `json:"height"`
}

func main() {

	//customer_details
	customerData := customer{
		Name:   "chamara",
		Age:    34,
		Height: 2.44,
	}

	byt, err := json.Marshal(customerData)

	client, _ := schemaregistry.NewClient("localhost:8081")
	//schema, _ := client.GetSchemaByID(10)
	schema, _ := client.GetSchemaBySubject("customer_details", 1)

	fmt.Println(schema)

	codec, err := goavro.NewCodec(schema.Schema)
	if err != nil {
		fmt.Println(err)
	}

	// Convert textual Avro data (in Avro JSON format) to native Go form
	native, _, err := codec.NativeFromTextual(byt)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// brokers := []string{"192.168.59.103:9092"}
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	topic := "first_topic"
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: 1,
		Key:       sarama.StringEncoder("test-key"),
		Value:     sarama.ByteEncoder(binary),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}
