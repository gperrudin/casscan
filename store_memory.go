package casscanner

import (
	"context"
	"sync"
)

// MemoryStore is a Store implementation backed by an in-memory store
type MemoryStore struct {
	lock sync.RWMutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string][]byte),
	}
}

func (m *MemoryStore) Load(ctx context.Context, key string) ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.data[key], nil
}

func (m *MemoryStore) LoadPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	res := make(map[string][]byte)
	for k, v := range m.data {
		if k[:len(prefix)] == prefix {
			res[k] = v
		}
	}
	return res, nil
}

func (m *MemoryStore) Store(ctx context.Context, key string, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.data[key] = value
	return nil
}
