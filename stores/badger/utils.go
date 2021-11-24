package badgerstore

import (
	"bytes"
	"encoding/gob"
	"tinyq"
)

// encode message
func encode(m tinyq.Message) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(m); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// decode message
func decode(data []byte) (tinyq.Message, error) {
	var m tinyq.Message
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return tinyq.Message{}, err
	}
	return m, nil
}
