package main

type Publisher interface {
	Connect() (Publisher, error)
	Disconnect() error
	Publish(body []byte) error
}

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
