package tinyq

import (
	"errors"
	"sync"

	"github.com/rs/xid"
)

var (
	ErrQueueIsPaused  = errors.New("queue is already paused")
	ErrQueueIsRunning = errors.New("queue is already running")
	ErrUnknownCommand = errors.New("unknown command")
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
	Stop  Command = "stop"
)

// Store
type Store interface {
	Enqueue(msg Message) error
	Dequeue() (Message, error)
	IsEmpty() error
	Notify(uuid string, merr error) error
	List(typ MessageStatus, offset, limit uint64) (Messages, error)
	Retry() error
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
	errs   chan error
	start  chan struct{}
	pause  chan struct{}
	exit   chan struct{}
}

// NewQueue
func NewQueue(store Store, autostart bool) *Queue {
	q := &Queue{
		l:      &sync.Mutex{},
		wg:     &sync.WaitGroup{},
		state:  running,
		store:  store,
		stream: make(chan Message, 1),
		errs:   make(chan error, 1),
		start:  make(chan struct{}, 1),
		pause:  make(chan struct{}, 1),
		exit:   make(chan struct{}, 1),
	}

	if !autostart {
		if err := q.Exec(Pause); err != nil {
			q.errs <- err
		}
	}

	q.routine()

	return q
}

// routine
func (q *Queue) routine() {
	q.wg.Add(1)
	go func() {
		for {
			select {
			case <-q.pause:
				select {
				case <-q.start:
				case <-q.exit:
					q.wg.Done()
					close(q.stream)
					close(q.exit)
					return
				}
			case <-q.exit:
				q.wg.Done()
				close(q.stream)
				close(q.exit)
				return
			default:
				if q.state == paused {
					if err := q.Exec(Pause); err != nil {
						q.errs <- err
					}
					continue
				}

				if err := q.store.IsEmpty(); err != nil {
					if err := q.Exec(Pause); err != nil {
						q.errs <- err
					}
					continue
				}

				msg, err := q.store.Dequeue()
				if err != nil {
					if err := q.Exec(Pause); err != nil {
						q.errs <- err
					}
					continue
				}

				q.stream <- msg
			}
		}
	}()
}

func (q *Queue) Exec(cmd Command) error {
	q.l.Lock()
	defer q.l.Unlock()

	switch cmd {
	case Start:
		if q.state == running {
			return ErrQueueIsRunning
		}
		q.start <- struct{}{}
		q.state = running
		return nil
	case Pause:
		if q.state == paused {
			return ErrQueueIsPaused
		}
		q.pause <- struct{}{}
		q.state = paused
		return nil
	case Stop:
		q.exit <- struct{}{}
		q.state = paused
		return nil
	default:
		return ErrUnknownCommand
	}
}

// Enqueue
func (q *Queue) Enqueue(obj interface{}) error {
	key := xid.New().String()
	msg, err := NewMessage(key, obj)
	if err != nil {
		return err
	}

	return q.store.Enqueue(msg)
}

// Dequeue
func (q *Queue) Dequeue() <-chan Message {
	return q.stream
}

// Retry
func (q *Queue) Retry() error {
	return q.store.Retry()
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

// Errors
func (q *Queue) Errs() <-chan error {
	return q.errs
}

// Close
func (q *Queue) Close() {
	if err := q.Exec(Stop); err != nil {
		q.errs <- err
	}
	q.wg.Wait()
	close(q.pause)
	close(q.start)
	close(q.errs)
	q.store.Close()
}
