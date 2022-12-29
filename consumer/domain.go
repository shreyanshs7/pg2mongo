package main

import "github.com/streadway/amqp"

type Consumer interface {
	SetupConsumer()
	ConsumeMessage(message amqp.Delivery)
}
