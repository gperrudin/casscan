package casscanner

import (
	"context"
	"database/sql"
)

// SQLStore is a Store implementation backed by a SQL store (table should be created beforehand)
// It uses the provided sql.DB as the database connection
// The table should have the following schema:
// CREATE TABLE scan_state (
//
//	id VARCHAR(255) PRIMARY KEY,
//	state BLOB
//
// );
type SQLStore struct {
	db *sql.DB
}

func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{
		db: db,
	}
}

func (s *SQLStore) Load(ctx context.Context, id string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, "SELECT state FROM scan_state WHERE id = ?", id).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *SQLStore) LoadPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, state FROM scan_state WHERE id LIKE ?", prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[string][]byte)
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		res[id] = data
	}
	return res, nil
}

func (s *SQLStore) Store(ctx context.Context, key string, value []byte) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO store (key, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?", key, value, value)
	return err
}
