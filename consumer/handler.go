package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConsumer struct {
}

func NewConsumer() Consumer {
	return &RabbitMQConsumer{}
}

func (rmqc *RabbitMQConsumer) SetupConsumer() <-chan amqp.Delivery {
	// Define RabbitMQ server URL.
	amqpServerURL := "amqp://admin:password@localhost:5672/?heartbeat=0"

	// Create a new RabbitMQ connection.
	connectRabbitMQ, err := amqp.Dial(amqpServerURL)
	if err != nil {
		panic(err)
	}
	// defer connectRabbitMQ.Close()

	// Opening a channel to our RabbitMQ instance over
	// the connection we have already established.
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		panic(err)
	}
	// defer channelRabbitMQ.Close()

	// Subscribing to pg2mongo_queue for getting messages.
	messages, err := channelRabbitMQ.Consume(
		"pg2mongo_queue", // queue name
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no local
		false,            // no wait
		nil,              // arguments
	)
	if err != nil {
		log.Println(err)
	}

	// Build a welcome message.
	log.Println("Successfully connected to RabbitMQ")
	log.Println("Waiting for messages")

	return messages
}

func (rmqc *RabbitMQConsumer) ConsumeMessage(body []byte) {
	// For example, show received message in a console.
	var logicalMsg Wal2JsonChange
	err := json.Unmarshal(body, &logicalMsg)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}
	log.Printf(" > Received message: %s\n", body)

	wal2JsonV2 := rmqc.ParseWALMessage(logicalMsg)
	s, _ := json.MarshalIndent(wal2JsonV2, "", "\t")
	fmt.Printf("Wal2JsonV2 %s", string(s))
}

func (rmqc *RabbitMQConsumer) ParseWALMessage(walMessage Wal2JsonChange) Wal2JsonV2 {
	var wal2JsonV2 Wal2JsonV2
	wal2JsonV2.Action, wal2JsonV2.Table, wal2JsonV2.Schema = walMessage.Action, walMessage.Table, walMessage.Schema

	wal2JsonV2.New = make(map[string]interface{})
	for _, col := range walMessage.Columns {
		wal2JsonV2.New[col.Name] = col.Value
	}
	if walMessage.Identity != nil {
		wal2JsonV2.Old = make(map[string]interface{})
		for _, col := range walMessage.Identity {
			wal2JsonV2.Old[col.Name] = col.Value
		}
	}
	return wal2JsonV2
}
