package casscanner

import (
	"context"
	badger "github.com/dgraph-io/badger/v4"
)

type FileStore struct {
	db *badger.DB
}

// NewFileStore returns a Store implementation backed by a file store
// It uses /tmp/casscanner_state as the default file path
func NewFileStore() *FileStore {
	db, err := badger.Open(badger.DefaultOptions("/tmp/casscanner_state"))
	if err != nil {
		panic(err)
	}
	return &FileStore{
		db: db,
	}
}

// NewFileStoreWithPath returns a Store implementation backed by a file store
// It uses the provided path as the file path
func NewFileStoreWithPath(path string) *FileStore {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		panic(err)
	}
	return &FileStore{
		db: db,
	}
}

func (f *FileStore) Load(ctx context.Context, id string) (value []byte, err error) {
	err = f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})
	return
}

func (f *FileStore) LoadPrefix(ctx context.Context, prefixId string) (map[string][]byte, error) {
	res := make(map[string][]byte)
	err := f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(prefixId)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			res[string(item.Key())] = value
		}
		return nil
	})

	return res, err
}

func (f *FileStore) Store(ctx context.Context, id string, data []byte) error {
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(id), data)
	})
}

var _ Store = (*FileStore)(nil)
