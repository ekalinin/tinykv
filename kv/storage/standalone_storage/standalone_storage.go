package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	db     *badger.DB
}

// NewStandAloneStorage creates a new storage
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		config: conf,
	}
	s.Start()
	return s
}

// Start a db server
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.config.DBPath
	opts.ValueDir = s.config.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Stop stops db
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Reader return a reader interface
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("Please, init db")
	}

	tx := s.db.NewTransaction(false)
	return saReader{tx: tx}, nil
}

// WriteX old implemetation (withour CF support)
func (s *StandAloneStorage) WriteX(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.New("Please, init db")
	}

	tx := s.db.NewTransaction(true)
	defer tx.Discard()
	for _, m := range batch {
		v := m.Value()
		if v == nil {
			if err := tx.Delete(m.Key()); err != nil {
				return err
			}
			continue
		}

		if err := tx.Set(m.Key(), v); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Write
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.New("Please, init db")
	}

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			return engine_util.DeleteCF(s.db, m.Cf(), m.Key())
		default:
			return errors.New("unsupported operation")
		}
	}

	return nil
}
