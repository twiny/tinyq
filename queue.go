package tinyq

import (
	"context"
	"fmt"
	"sync"

	"github.com/twiny/dice"
)

// State
type State string

const (
	running State = "running"
	paused  State = "paused"
)

type Command string

const (
	Start Command = "start"
	Pause Command = "pause"
	Retry Command = "retry"
	Stop  Command = "stop"
)

// Store
type Store interface {
	Enqueue(msg Message) error
	Dequeue() (Message, error)
	Retry() error
	Notify(uuid string, merr error) error
	List(typ MessageStatus, offset, limit uint64) (Messages, error)
	IsEmpty() bool
	Remove(typ MessageStatus) error
	Statistic() (Stats, error)
	Close()
}

// Queue
type Queue struct {
	l      *sync.Mutex
	wg     *sync.WaitGroup
	state  State
	store  Store
	stream chan Message
	start  chan struct{}
	pause  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// NewQueue
func NewQueue(store Store, autostart bool) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		l:      &sync.Mutex{},
		wg:     &sync.WaitGroup{},
		state:  running,
		store:  store,
		stream: make(chan Message, 1), // todo
		start:  make(chan struct{}, 1),
		pause:  make(chan struct{}, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	if !autostart {
		q.pause <- struct{}{}
		q.state = paused
	}

	q.routine()

	return q
}

// routine
func (q *Queue) routine() {
	q.wg.Add(1)
	go func() {
		defer func() {
			q.wg.Done()
			close(q.stream)
		}()
		//
		for {
			select {
			case <-q.pause:
				select {
				case <-q.start:
				case <-q.ctx.Done():
					return
				}
			case <-q.ctx.Done():
				return
			default:
				if q.ctx.Err() != nil {
					return
				}

				if q.state == paused {
					q.pause <- struct{}{}
					q.state = paused
					continue
				}

				if q.store.IsEmpty() {
					q.pause <- struct{}{}
					q.state = paused
					continue
				}

				msg, err := q.store.Dequeue()
				if err != nil {
					q.pause <- struct{}{}
					q.state = paused
					continue
				}

				q.stream <- msg // this blocks
			}
		}
	}()
}

// Exec
func (q *Queue) Exec(cmd Command) error {
	q.l.Lock()
	defer q.l.Unlock()

	switch cmd {
	case Start:
		if q.state == running {
			return fmt.Errorf("queue is already running")
		}
		if q.store.IsEmpty() {
			return fmt.Errorf("queue is empty")
		}

		q.state = running
		q.start <- struct{}{}
		return nil
	case Pause:
		if q.state == paused {
			return fmt.Errorf("queue is already paused")
		}
		q.pause <- struct{}{}
		q.state = paused
		return nil
	case Retry:
		return q.store.Retry()
	case Stop:
		if q.state == running {
			q.pause <- struct{}{}
			q.state = paused
			// this should handle when queue is closed
			// and queue is running. gracefule shutdown.
			msg := <-q.stream
			_ = q.Notify(msg.UUID, fmt.Errorf("queue is stopped")) // TODO: handle err
		}
		q.cancel()
		return nil
	default:
		return fmt.Errorf("unknown command")
	}
}

// Enqueue
func (q *Queue) Enqueue(v interface{}) error {
	key := dice.RandString(25)

	msg, err := NewMessage(key, v)
	if err != nil {
		return err
	}

	return q.store.Enqueue(msg)
}

// Dequeue
func (q *Queue) Dequeue() <-chan Message {
	return q.stream
}

// Notify
func (q *Queue) Notify(uuid string, merr error) error {
	return q.store.Notify(uuid, merr)
}

// List
func (q *Queue) List(typ MessageStatus, offset, limit uint64) (Messages, error) {
	return q.store.List(typ, offset, limit)
}

// Remove
func (q *Queue) Remove(typ MessageStatus) error {
	return q.store.Remove(typ)
}

// Stats
type Stats struct {
	IsRunning bool   `json:"is_running"`
	Pending   uint64 `json:"pending"`
	Failed    uint64 `json:"failed"`
}

// Statistic
func (q *Queue) Statistic() (Stats, error) {
	stats, err := q.store.Statistic()
	if err != nil {
		return stats, err
	}

	stats.IsRunning = q.state == running

	return stats, nil
}

// Close
func (q *Queue) Close() {
	_ = q.Exec(Stop)
	close(q.pause)
	close(q.start)
	// q.wg.Wait()
	q.store.Close()
}
