package boltstore

import (
	"errors"
	"sync"
	"sync/atomic"

	json "github.com/goccy/go-json"
	"github.com/twiny/tinyq"

	"go.etcd.io/bbolt"
)

var (
	ErrEmptyQueue      = errors.New("queue is empty")
	ErrMessageNotFound = errors.New("message not found")
	ErrUnknownType     = errors.New("unknown message type")
)

// Store
type Store struct {
	lane *Lane
	db   *bbolt.DB
}

// NewStore
func NewStore(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0644, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}

	// create buckets
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("pending")) // pending messages
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("failed")) // failed messages
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	store := &Store{
		lane: &Lane{
			mu:    &sync.Mutex{},
			slice: []string{},
		},
		db: db,
	}

	if err := store.requeue(); err != nil {
		return nil, err
	}

	return store, nil
}

// Requeue
func (s *Store) requeue() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("pending"))

		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			s.lane.add(string(k))
		}
		return nil
	})
}

// Enqueue
func (s *Store) Enqueue(msg tinyq.Message) error {
	if err := s.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("pending"))

		body, err := encode(msg)
		if err != nil {
			return err
		}

		if err := bucket.Put([]byte(msg.UUID), body); err != nil {
			return err
		}

		s.lane.add(msg.UUID)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Dequeue
func (s *Store) Dequeue() (tinyq.Message, error) {
	uuid, err := s.lane.pop()
	if err != nil {
		return tinyq.Message{}, err
	}

	var msg tinyq.Message
	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("pending"))

		val := bucket.Get([]byte(uuid))
		if val == nil {
			return ErrMessageNotFound
		}

		if err := json.Unmarshal(val, &msg); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return tinyq.Message{}, err
	}
	return msg, nil
}

// IsEmpty
func (s *Store) IsEmpty() error {
	if len(s.lane.slice) == 0 {
		return ErrEmptyQueue
	}
	return nil
}

// Notify
func (s *Store) Notify(uuid string, merr error) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("pending"))

		// get message
		val := bucket.Get([]byte(uuid))
		if val == nil {
			return ErrMessageNotFound
		}

		var msg tinyq.Message
		if err := decode(val, &msg); err != nil {
			return err
		}

		if err := bucket.Delete([]byte(uuid)); err != nil {
			return err
		}

		if merr != nil {
			bucket := tx.Bucket([]byte("failed"))
			msg.Detail = merr.Error()
			b, err := encode(msg)
			if err != nil {
				return err
			}
			return bucket.Put([]byte(msg.UUID), b)
		}

		return nil
	})
}

// List
func (s *Store) List(typ tinyq.MessageStatus, offset, limit uint64) (tinyq.Messages, error) {
	switch typ {
	case tinyq.Pending:
		return s.listPending(offset, limit)
	case tinyq.Failed:
		return s.listFailed(offset, limit)
	}
	return tinyq.Messages{}, ErrUnknownType
}

// listPending
func (s *Store) listPending(offset, limit uint64) (tinyq.Messages, error) {
	var msgs = tinyq.Messages{
		// Total: ,
		Offset: offset,
		Limit:  limit,
		Page:   (offset / limit) + 1,
		Items:  []tinyq.Message{},
	}

	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("pending"))
		curser := bucket.Cursor()

		var totalCount, limitCount, offsetCount uint64 = 0, 0, 0

		// totalCount
		for k, v := curser.First(); k != nil; k, v = curser.Next() {
			if k != nil && v != nil {
				atomic.AddUint64(&totalCount, 1)
			}
		}
		msgs.Total = totalCount

		for k, v := curser.First(); k != nil; k, v = curser.Next() {
			var msg tinyq.Message
			if err := decode(v, &msg); err != nil {
				return err
			}
			// increment off set
			atomic.AddUint64(&offsetCount, 1)
			if offset <= offsetCount {
				// increment limit
				atomic.AddUint64(&limitCount, 1)
				if limit >= limitCount {
					msgs.Items = append(msgs.Items, msg)
				}
				// break when limit reached
				if limit == limitCount+1 {
					// return nil
					break
				}
			}
		}
		return nil
	}); err != nil {
		return tinyq.Messages{}, err
	}

	return msgs, nil
}

// listFailed
func (s *Store) listFailed(offset, limit uint64) (tinyq.Messages, error) {
	var msgs = tinyq.Messages{
		// Total: ,
		Offset: offset,
		Limit:  limit,
		Page:   (offset / limit) + 1,
		Items:  []tinyq.Message{},
	}

	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("failed"))
		curser := bucket.Cursor()

		var totalCount, limitCount, offsetCount uint64 = 0, 0, 0

		// totalCount
		for k, v := curser.First(); k != nil; k, v = curser.Next() {
			if k != nil && v != nil {
				atomic.AddUint64(&totalCount, 1)
			}
		}
		msgs.Total = totalCount

		for k, v := curser.First(); k != nil; k, v = curser.Next() {
			var msg tinyq.Message
			if err := decode(v, &msg); err != nil {
				continue
			}
			// increment off set
			atomic.AddUint64(&offsetCount, 1)

			if offset <= offsetCount {
				// increment limit
				atomic.AddUint64(&limitCount, 1)
				if limit >= limitCount {
					msgs.Items = append(msgs.Items, msg)
				}

				// break when limit reached
				if limit == limitCount {
					// return nil
					break
				}
			}
		}
		return nil
	}); err != nil {
		return tinyq.Messages{}, err
	}

	return msgs, nil
}

// Remove
func (s *Store) Remove(typ tinyq.MessageStatus) error {
	switch typ {
	case tinyq.Pending:
		return s.removePending()
	case tinyq.Failed:
		return s.removeFailed()
	}
	return nil
}

// removePending
func (s *Store) removePending() error {
	// clean pending land
	s.lane.clear()

	//
	return s.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte([]byte("pending")))
		curser := bucket.Cursor()
		for k, _ := curser.First(); k != nil; k, _ = curser.Next() {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// removeFailed
func (s *Store) removeFailed() error {
	return s.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte([]byte("failed")))
		curser := bucket.Cursor()
		for k, _ := curser.First(); k != nil; k, _ = curser.Next() {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// Retry
func (s *Store) Retry() error {
	return s.db.Batch(func(tx *bbolt.Tx) error {
		failedB := tx.Bucket([]byte("failed"))
		pendingB := tx.Bucket([]byte("pending"))

		c := failedB.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := pendingB.Put(k, v); err != nil {
				return err
			}

			s.lane.add(string(k))

			if err := failedB.Delete(k); err != nil {
				return err
			}

		}
		return nil
	})
}

// Statistic
func (s *Store) Statistic() (tinyq.Stats, error) {
	var stats tinyq.Stats
	// count failed
	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte([]byte("failed")))
		curser := bucket.Cursor()
		for k, _ := curser.First(); k != nil; k, _ = curser.Next() {
			if k != nil {
				atomic.AddUint64(&stats.Failed, 1)
			}
		}
		return nil
	}); err != nil {
		return stats, err
	}

	// count pending
	if err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte([]byte("pending")))
		curser := bucket.Cursor()
		for k, _ := curser.First(); k != nil; k, _ = curser.Next() {
			if k != nil {
				atomic.AddUint64(&stats.Pending, 1)
			}
		}
		return nil
	}); err != nil {
		return tinyq.Stats{}, err
	}

	return stats, nil
}

// close
func (s *Store) Close() {
	s.db.Close()
}
