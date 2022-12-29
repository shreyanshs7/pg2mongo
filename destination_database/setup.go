package destinationdatabase

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type DBClient struct {
	dbClient *mongo.Client
}

var dbClient DBClient

func SetupDestinationDB() *mongo.Client {
	ctx := context.TODO()
	credential := options.Credential{
		Username: "test_user",
		Password: "test12345",
	}
	options := options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(credential)
	client, err := mongo.Connect(ctx, options)
	if err != nil {
		log.Fatal("Error while connecting with mongo", err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal("Error while trying to ping mongo", err)
	}
	log.Println("Successfully connected to database")
	setClient(client)
	return client
}

func DisconnectDB(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
		log.Fatal("Error while disconnecting from mongo", err)
	}
}

func setClient(client *mongo.Client) {
	dbClient.dbClient = client
}

func GetClient() *mongo.Client {
	return dbClient.dbClient
}
