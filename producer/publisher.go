package main

import (
	"github.com/streadway/amqp"
)

type RabbitMQPublisher struct {
	channel *amqp.Channel
}

func NewPublisher() Publisher {
	return &RabbitMQPublisher{}
}

func (rmqp *RabbitMQPublisher) Connect() (Publisher, error) {
	// Connecting to rabbitmq
	amqpServerURL := "amqp://admin:password@localhost:5672/?heartbeat=0"

	// Create a new RabbitMQ connection.
	connectRabbitMQ, err := amqp.Dial(amqpServerURL)
	if err != nil {
		return nil, err
	}

	// Let's start by opening a channel to our RabbitMQ
	// instance over the connection we have already
	// established.
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		return nil, err
	}

	// With the instance and declare Queues that we can
	// publish and subscribe to.
	_, err = channelRabbitMQ.QueueDeclare(
		"pg2mongo_queue", // queue name
		true,             // durable
		false,            // auto delete
		false,            // exclusive
		false,            // no wait
		nil,              // arguments
	)
	if err != nil {
		return nil, err
	}
	rmqp.channel = channelRabbitMQ
	return rmqp, nil
}

func (rmqp *RabbitMQPublisher) Disconnect() error {
	return nil
}

func (rmqp *RabbitMQPublisher) Publish(body []byte) error {
	message := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}
	err := rmqp.channel.Publish(
		"",               // exchange
		"pg2mongo_queue", // queue name
		false,            // mandatory
		false,            // immediate
		message,          // message to publish
	)
	return err
}
