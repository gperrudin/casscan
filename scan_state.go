package casscanner

import (
	"context"
	"encoding/json"
	"fmt"
)

type scanState struct {
	Token         int64
	ScanRowsCount int64
	Finished      bool
}

type scanStateStore struct {
	underlying Store
}

func newScanStateStore(store Store) scanStateStore {
	return scanStateStore{
		underlying: store,
	}
}

func (s *scanStateStore) load(ctx context.Context, id string) (*scanState, error) {
	data, err := s.underlying.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	var state scanState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("could not decode scan state: %w", err)
	}
	return &state, nil
}

func (s *scanStateStore) store(ctx context.Context, id string, state *scanState) error {
	var encoded []byte
	if state != nil {
		var err error
		encoded, err = json.Marshal(state)
		if err != nil {
			return fmt.Errorf("could not encode scan state: %w", err)
		}
	}
	return s.underlying.Store(ctx, id, encoded)
}

func (s *scanStateStore) loadPrefix(ctx context.Context, prefixId string) (map[string]*scanState, error) {
	data, err := s.underlying.LoadPrefix(ctx, prefixId)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*scanState, len(data))
	for k, v := range data {
		var state scanState
		if err := json.Unmarshal(v, &state); err != nil {
			return nil, fmt.Errorf("could not decode scan state: %w", err)
		}
		res[k] = &state
	}

	return res, nil
}
