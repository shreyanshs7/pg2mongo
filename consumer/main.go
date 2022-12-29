// ./consumer/main.go

package main

import destinationdatabase "pg2mongo/destination_database"

type Wal2JsonChange struct {
	Action   string   `json:"action"`
	Schema   string   `json:"schema"`
	Table    string   `json:"table"`
	Columns  []Column `json:"columns"`
	Identity []Column `json:"identity"`
}

type Column struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

func main() {

	messages := SetupRabbitMQConsumer()
	_ = destinationdatabase.SetupDestinationDB()

	// Make a channel to receive messages into infinite loop.
	forever := make(chan bool)

	go func() {
		for message := range messages {
			ConsumeMessage(message)
		}
	}()

	<-forever
}
