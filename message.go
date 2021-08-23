package tinyq

import (
	json "github.com/goccy/go-json"
)

// MessageStatus
type MessageStatus string

const (
	Pending MessageStatus = "pending"
	Success MessageStatus = "success"
	Failed  MessageStatus = "failed"
)

// Message
type Message struct {
	UUID   string        `json:"uuid"`
	Status MessageStatus `json:"status"`
	Body   []byte        `json:"body"`
	Detail string        `json:"detail"`
}

// Messages
type Messages struct {
	Total  uint64    `json:"total"`
	Offset uint64    `json:"offset"`
	Limit  uint64    `json:"limit"`
	Page   uint64    `json:"page"`
	Items  []Message `json:"items"`
}

// NewMessage
func NewMessage(key string, data interface{}) (Message, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return Message{}, err
	}

	return Message{
		UUID:   key,
		Status: Pending,
		Body:   body,
		Detail: "",
	}, nil
}

// Data
func (msg Message) Value(data interface{}) error {
	return json.Unmarshal(msg.Body, data)
}
