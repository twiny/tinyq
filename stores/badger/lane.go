package badgerstore

import (
	"fmt"
	"sync"
)

// Lane
type lane struct {
	mu    *sync.Mutex
	slice []string
}

// add
func (l *lane) add(uuid string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.slice = append(l.slice, uuid)
}

// pop
func (l *lane) pop() (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.slice) == 0 {
		return "", fmt.Errorf("queue is empty")
	}

	uuid := l.slice[0]
	l.slice = l.slice[1:]

	return uuid, nil
}

// clear
func (l *lane) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slice = []string{}
}
