package tinyq

import (
	"bytes"
	"encoding/gob"
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
func NewMessage(key string, b interface{}) (Message, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(b); err != nil {
		return Message{}, err
	}

	return Message{
		UUID:   key,
		Status: Pending,
		Body:   buf.Bytes(),
		Detail: "",
	}, nil
}

// Data
func (msg Message) Value(v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(msg.Body)).Decode(v)
}
