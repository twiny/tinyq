package badgerstore

import (
	"bytes"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"
	"tinyq"

	"github.com/dgraph-io/badger/v3"
)

// Store - badgerdb
type Store struct {
	db   *badger.DB
	lane *lane
}

// NewBadgerStore
func NewBadgerStore(dir string) (*Store, error) {
	opt := badger.DefaultOptions(dir)
	opt.ValueDir = path.Join(dir, "data")
	opt.Logger = nil

	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	// Badger GC
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
		again:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	s := &Store{
		db: db,
		lane: &lane{
			mu:    &sync.Mutex{},
			slice: []string{},
		},
	}

	go s.requeue()

	return s, nil
}

// Requeue
func (s *Store) requeue() error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(tinyq.Pending)); it.ValidForPrefix([]byte(tinyq.Pending)); it.Next() {
			item := it.Item()
			key := item.Key()

			s.lane.add(string(key))
		}

		return nil
	})
}

// Enqueue
func (s *Store) Enqueue(msg tinyq.Message) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val, err := encode(msg)
		if err != nil {
			return err
		}

		key := bytes.Join([][]byte{[]byte(tinyq.Pending), []byte(msg.UUID)}, []byte("_"))

		s.lane.add(string(key))

		return txn.Set(key, val)
	})
}

// Dequeue
func (s *Store) Dequeue() (tinyq.Message, error) {
	key, err := s.lane.pop()
	if err != nil {
		return tinyq.Message{}, err
	}

	if err != nil {
		return tinyq.Message{}, err
	}

	var msg tinyq.Message
	if err := s.db.View(func(txn *badger.Txn) error {

		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			msg, err = decode(val)
			return err
		})
	}); err != nil {
		return tinyq.Message{}, err
	}

	return msg, nil
}

// IsEmpty
func (s *Store) IsEmpty() error {
	if len(s.lane.slice) == 0 {
		return fmt.Errorf("queue is empty")
	}
	return nil
}

// Notify
func (s *Store) Notify(uuid string, merr error) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := bytes.Join([][]byte{[]byte(tinyq.Pending), []byte(uuid)}, []byte("_"))

		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		//
		var val []byte
		if err := item.Value(func(v []byte) error {
			val = v
			return nil
		}); err != nil {
			return err
		}

		m, err := decode(val)
		if err != nil {
			return err
		}

		if err := txn.Delete(key); err != nil {
			return err
		}

		if merr != nil {
			m.Status = tinyq.Failed
			m.Detail = merr.Error()
			key := bytes.Join([][]byte{[]byte(tinyq.Failed), []byte(uuid)}, []byte("_"))
			if err := txn.Set(key, val); err != nil {
				return err
			}
			return nil
		}

		return nil
	})
}

// List
func (s *Store) List(typ tinyq.MessageStatus, offset, limit uint64) (tinyq.Messages, error) {
	switch typ {
	case tinyq.Pending:
		return s.listPendingMessages(offset, limit)
	case tinyq.Failed:
		return s.listFailedMessages(offset, limit)
	}
	return tinyq.Messages{}, fmt.Errorf("unknow status")
}

// listPendingMessages
func (s *Store) listPendingMessages(offset, limit uint64) (tinyq.Messages, error) {
	var msgs = tinyq.Messages{
		// Total: ,
		Offset: offset,
		Limit:  limit,
		Page:   (offset / limit) + 1,
		Items:  []tinyq.Message{},
	}

	// counter
	var totalCount, limitCount, offsetCount uint64 = 0, 0, 0

	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		// total pending count
		for it.Seek([]byte(tinyq.Pending)); it.ValidForPrefix([]byte(tinyq.Pending)); it.Next() {
			atomic.AddUint64(&totalCount, 1)
		}

		msgs.Total = totalCount

		// pagination
		for it.Seek([]byte(tinyq.Pending)); it.ValidForPrefix([]byte(tinyq.Pending)); it.Next() {
			// increment off set
			atomic.AddUint64(&offsetCount, 1)

			if offset <= offsetCount {
				// increment limit
				atomic.AddUint64(&limitCount, 1)
				if limit >= limitCount {
					item := it.Item()
					if err := item.Value(func(val []byte) error {
						msg, err := decode(val)
						msgs.Items = append(msgs.Items, msg)
						return err
					}); err != nil {
						return err
					}
				}

				// break when limit reached
				if limit == limitCount {
					return nil
					// break
				}
			}
		}

		return nil
	}); err != nil {
		return tinyq.Messages{}, err
	}

	return msgs, nil
}

// listFailedMessages
func (s *Store) listFailedMessages(offset, limit uint64) (tinyq.Messages, error) {
	var msgs = tinyq.Messages{
		// Total: ,
		Offset: offset,
		Limit:  limit,
		Page:   (offset / limit) + 1,
		Items:  []tinyq.Message{},
	}

	// counter
	var totalCount, limitCount, offsetCount uint64 = 0, 0, 0

	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		// total failed count
		for it.Seek([]byte(tinyq.Failed)); it.ValidForPrefix([]byte(tinyq.Failed)); it.Next() {
			atomic.AddUint64(&totalCount, 1)
		}

		msgs.Total = totalCount

		// pagination
		for it.Seek([]byte(tinyq.Failed)); it.ValidForPrefix([]byte(tinyq.Failed)); it.Next() {
			// increment off set
			atomic.AddUint64(&offsetCount, 1)

			if offset <= offsetCount {
				// increment limit
				atomic.AddUint64(&limitCount, 1)
				if limit >= limitCount {
					item := it.Item()
					if err := item.Value(func(val []byte) error {
						msg, err := decode(val)
						msgs.Items = append(msgs.Items, msg)
						return err
					}); err != nil {
						return err
					}
				}

				// break when limit reached
				if limit == limitCount {
					return nil
					// break
				}
			}
		}

		return nil
	}); err != nil {
		return tinyq.Messages{}, err
	}

	return msgs, nil
}

// Retry
func (s *Store) Retry() error {
	return fmt.Errorf("not implemented")
}

// Remove
func (s *Store) Remove(typ tinyq.MessageStatus) error {
	return fmt.Errorf("not implemented")
}

// Statistic
func (s *Store) Statistic() (tinyq.Stats, error) {
	var stats tinyq.Stats
	// count failed
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		// total failed count
		for it.Seek([]byte(tinyq.Failed)); it.ValidForPrefix([]byte(tinyq.Failed)); it.Next() {
			atomic.AddUint64(&stats.Failed, 1)
		}

		return nil
	}); err != nil {
		return tinyq.Stats{}, err
	}

	// count pending
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		// total pending count
		for it.Seek([]byte(tinyq.Pending)); it.ValidForPrefix([]byte(tinyq.Pending)); it.Next() {
			atomic.AddUint64(&stats.Pending, 1)
		}

		return nil
	}); err != nil {
		return tinyq.Stats{}, err
	}

	return stats, nil
}

// Close
func (s *Store) Close() {
	s.db.Close()
}
