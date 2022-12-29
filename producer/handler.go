package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type WalHandler struct {
	publisher Publisher
}

func NewWalHandler(publisher Publisher) *WalHandler {
	return &WalHandler{publisher: publisher}
}

func (walh *WalHandler) SetupReplication() (*pgconn.PgConn, pglogrepl.IdentifySystemResult) {
	const outputPlugin = "wal2json"
	conn, err := pgconn.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/dummy?replication=database")
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}

	result := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pg2mongo;")
	_, err = result.ReadAll()
	if err != nil {
		log.Fatalln("drop publication if exists error", err)
	}

	result = conn.Exec(context.Background(), "CREATE PUBLICATION pg2mongo FOR ALL TABLES;")
	_, err = result.ReadAll()
	if err != nil {
		log.Fatalln("create publication error", err)
	}
	log.Println("create publication pg2mongo")

	var pluginArguments []string = []string{"\"pretty-print\" 'true'", "\"format-version\" '2'"}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "pg2mongo"

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	return conn, sysident
}

func (walh *WalHandler) ConsumeWAL(conn *pgconn.PgConn, sysident pglogrepl.IdentifySystemResult, publisher Publisher) {
	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		clientXLogPos, nextStandbyMessageDeadline = walh.HandleWAL(msg, clientXLogPos, nextStandbyMessageDeadline)
	}
}

func (walh *WalHandler) HandleWAL(msg *pgproto3.CopyData, clientXLogPos pglogrepl.LSN, nextStandbyMessageDeadline time.Time) (pglogrepl.LSN, time.Time) {

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
		}
		log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

		if pkm.ReplyRequested {
			nextStandbyMessageDeadline = time.Time{}
		}

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			log.Fatalln("ParseXLogData failed:", err)
		}
		// log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n%s\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime, hex.Dump(xld.WALData))
		var logicalMsg Wal2JsonChange
		err = json.Unmarshal(xld.WALData, &logicalMsg)
		if err != nil {
			log.Fatalf("Parse logical replication message: %s", err)
		}
		log.Printf("Receive a logical replication message: %s\n", logicalMsg)

		s, _ := json.MarshalIndent(logicalMsg, "", "\t")
		fmt.Printf("Change message %s", string(s))

		if logicalMsg.Action != "B" && logicalMsg.Action != "C" {
			// Attempt to publish a message to the queue.
			err := walh.publisher.Publish(xld.WALData)
			if err != nil {
				fmt.Println("Failed to publish message ", err)
			}
		}
		clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	}
	return clientXLogPos, nextStandbyMessageDeadline
}
