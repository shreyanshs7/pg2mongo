package main

import "github.com/streadway/amqp"

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

type Wal2JsonV2 struct {
	Action string                    `json:"action"`
	Schema string                    `json:"schema"`
	Table  string                    `json:"table"`
	Old    map[string]interface{}    `json:"old"`
	New    map[string]interface{}    `json:"new"`
	Diff   map[string]Wal2JsonV2Diff `json:"diff"`
}

type Wal2JsonV2Diff struct {
	Old interface{} `json:"old"`
	New interface{} `json:"new"`
}

type Consumer interface {
	SetupConsumer() <-chan amqp.Delivery
	ConsumeMessage(body []byte)
	ParseWALMessage(walMessaeg Wal2JsonChange) Wal2JsonV2
}
