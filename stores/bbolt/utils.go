package boltstore

import (
	json "github.com/goccy/go-json"
	"github.com/twiny/tinyq"
)

// encode
func encode(msg tinyq.Message) ([]byte, error) {
	return json.Marshal(msg)
}

// decode
func decode(b []byte, msg *tinyq.Message) error {
	if err := json.Unmarshal(b, msg); err != nil {
		return err
	}
	return nil
}
