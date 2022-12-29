package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQConsumer struct {
}

func SetupRabbitMQConsumer() <-chan amqp.Delivery {
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

	// Subscribing to QueueService1 for getting messages.
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

func ConsumeMessage(message amqp.Delivery) {
	// For example, show received message in a console.
	var logicalMsg Wal2JsonChange
	err := json.Unmarshal(message.Body, &logicalMsg)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}
	log.Printf(" > Received message: %s\n", message.Body)
}
