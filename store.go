package casscanner

import (
	"context"
)

// Store is an interface for storing and retrieving key-value pairs.
type Store interface {
	// Load retrieves the value for the given key.
	Load(ctx context.Context, key string) ([]byte, error)
	// LoadPrefix retrieves all key-value pairs with the given key prefix.
	LoadPrefix(ctx context.Context, prefix string) (map[string][]byte, error)
	// Store stores the value for the given key.
	Store(ctx context.Context, key string, val []byte) error
}
