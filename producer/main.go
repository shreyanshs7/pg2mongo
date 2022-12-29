package main

import (
	"log"
)

func main() {
	publisher, err := NewPublisher().Connect()
	if err != nil {
		log.Fatalln("Failed to connect to publisher")
	}
	// Replication
	walHandler := NewWalHandler(publisher)
	conn, sysident := walHandler.SetupReplication()

	walHandler.ConsumeWAL(conn, sysident, publisher)
}
