package boltstore

import (
	"sync"
)

// Lane
type Lane struct {
	mu    *sync.Mutex
	slice []string
}

// add
func (l *Lane) add(uuid string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.slice = append(l.slice, uuid)
}

// pop
func (l *Lane) pop() (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.slice) == 0 {
		return "", ErrEmptyQueue
	}

	uuid := l.slice[0]
	l.slice = l.slice[1:]

	return uuid, nil
}

// clear
func (l *Lane) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slice = []string{}
}
