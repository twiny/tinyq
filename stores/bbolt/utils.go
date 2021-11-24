package boltstore

import (
	"bytes"
	"encoding/gob"
	"tinyq"
)

// encode
func encode(msg tinyq.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decode
func decode(b []byte) (tinyq.Message, error) {
	var msg tinyq.Message
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&msg); err != nil {
		return tinyq.Message{}, err
	}
	return msg, nil
}
