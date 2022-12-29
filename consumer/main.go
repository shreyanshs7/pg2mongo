// ./consumer/main.go

package main

// import destinationdatabase "pg2mongo/destination_database"

func main() {
	consumer := NewConsumer()
	messages := consumer.SetupConsumer()
	// _ = destinationdatabase.SetupDestinationDB()

	// Make a channel to receive messages into infinite loop.
	forever := make(chan bool)

	go func() {
		for message := range messages {
			consumer.ConsumeMessage(message.Body)
		}
	}()

	<-forever
}
