package casscanner

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"math"
	"strconv"
	"strings"
)

type Scanner struct {
	stateStore scanStateStore
	session    *gocql.Session
}

type query struct {
	stmt   string
	values []interface{}
	rng    tokenRange
}

func NewScanner(stateStore Store, session *gocql.Session) *Scanner {
	return &Scanner{
		stateStore: newScanStateStore(stateStore),
		session:    session,
	}
}

// Iter creates a new iterator for the given scanId and query. The query should be a SELECT statement.
func (s *Scanner) Iter(ctx context.Context, scanId string, stmt string, values ...interface{}) (*Iter, error) {
	state, err := s.stateStore.load(ctx, scanId)
	if err != nil {
		return nil, err
	}

	var lastTokenPtr *int64
	if state != nil {
		lastTokenPtr = &state.Token
	}

	q := query{
		stmt:   stmt,
		values: values,
		rng: tokenRange{
			from: lastTokenPtr,
			to:   nil,
		},
	}
	return s.buildIter(ctx, scanId, q)
}

// SplitIter creates multiple iterators for the given scanId and query. The query should be a SELECT statement.
// It allows to read the data in parallel by splitting the cassandra token ring in `splits` parts.
func (s *Scanner) SplitIter(ctx context.Context, scanId string, splits int, stmt string, values ...interface{}) (Iters, error) {
	iters := make(Iters, splits)

	for i, rng := range splitTokenRing(splits) {
		q := query{
			stmt:   stmt,
			values: values,
			rng:    rng,
		}

		var err error
		iters[i], err = s.buildIter(ctx, scanId+"_"+strconv.Itoa(i), q)
		if err != nil {
			return nil, err
		}
	}

	return iters, nil
}

func (s *Scanner) buildIter(ctx context.Context, scanId string, q query) (*Iter, error) {
	gocqlQuery, err := s.buildQuery(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("could not build query: %w", err)
	}

	it := Iter{
		scanner: s,

		ctx:    ctx,
		scanId: scanId,
		query:  q,
		iter:   gocqlQuery.Iter(),
	}

	return &it, nil
}

func (s *Scanner) buildQuery(ctx context.Context, q query) (*gocql.Query, error) {
	parsed, err := parceCQLQuery(q.stmt)
	if err != nil {
		return nil, fmt.Errorf("could not parse query: %w", err)
	}

	pkColumns, err := getPrimaryKeyColumns(s.session, parsed.keyspace, parsed.table)
	if err != nil {
		return nil, fmt.Errorf("could not get primary key columns: %w", err)
	}

	if q.rng.from != nil || q.rng.to != nil {
		if q.rng.from != nil {
			fromTokenClause := fmt.Sprintf("token(%s) >= %d", strings.Join(pkColumns, ", "), *q.rng.from)
			parsed.addWhere(fromTokenClause)
		}
		if q.rng.to != nil {
			toTokenClause := fmt.Sprintf("token(%s) < %d", strings.Join(pkColumns, ", "), *q.rng.to)
			parsed.addWhere(toTokenClause)
		}
	}

	parsed.columnsPart += fmt.Sprintf(", token(%s)", strings.Join(pkColumns, ", "))

	return s.session.Query(parsed.String(), q.values...).WithContext(ctx), nil
}

// getPrimaryKeyColumns returns the primary key columns for the given table.
func getPrimaryKeyColumns(s *gocql.Session, keyspace, table string) ([]string, error) {
	keyspaceMetadata, err := s.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, err
	}
	tableMetadata := keyspaceMetadata.Tables[table]
	if tableMetadata == nil {
		return nil, fmt.Errorf("could not find metadata for table: %s.%s", keyspace, table)
	}

	pkColumns := make([]string, 0)
	for _, pkColumn := range tableMetadata.PartitionKey {
		pkColumns = append(pkColumns, pkColumn.Name)
	}

	return pkColumns, nil
}

func (s *Scanner) store(ctx context.Context, id string, state *scanState) error {
	return s.stateStore.store(ctx, id, state)
}

type Iter struct {
	scanner *Scanner
	ctx     context.Context

	scanId string
	query  query
	iter   *gocql.Iter

	state scanState
}

// Scan is a wrapper around gocql.Iter.Scan
func (it *Iter) Scan(dest ...interface{}) bool {
	if it.state.Finished {
		return false
	}

	values := append(dest, &it.state.Token)
	if it.iter.Scan(values...) {
		it.state.ScanRowsCount++
		return true
	} else {
		it.state.Finished = true
		return false
	}
}

// Save stores the current state of the iterator.
func (it *Iter) Save() error {
	return it.scanner.store(it.ctx, it.scanId, &it.state)
}

// Reset resets the iterator to the initial state.
func (it *Iter) Reset() error {
	if err := it.scanner.store(it.ctx, it.scanId, nil); err != nil {
		return err
	}

	newIt, err := it.scanner.buildIter(it.ctx, it.scanId, it.query)
	if err != nil {
		return err
	}

	*it = *newIt
	return nil
}

func (it *Iter) Close() error {
	return it.iter.Close()
}

// ReadCount returns the number of rows read by the iterator.
func (it *Iter) ReadCount() int64 {
	return it.state.ScanRowsCount
}

// Finished returns true if the iterator has finished reading all the rows.
func (it *Iter) Finished() bool {
	return it.state.Finished
}

// Progress returns the progress of the iterator. It returns a value between 0 and 1.
func (it *Iter) Progress() float64 {
	if it.Finished() {
		return 1
	}
	if it.ReadCount() == 0 {
		return 0
	}

	rng := tokenRange{
		from: it.query.rng.to,
		to:   it.query.rng.to,
	}
	if rng.from == nil {
		min := int64(math.MinInt64)
		rng.from = &min
	}
	if rng.to == nil {
		max := int64(math.MaxInt64)
		rng.to = &max
	}

	if it.state.Token < *rng.from {
		return 0
	}
	if it.state.Token >= *rng.to {
		return 1
	}

	return float64(it.state.Token-*rng.from) / float64(*rng.to-*rng.from)
}

// EstimatedCount returns the estimated number of rows that the iterator will read.
func (it *Iter) EstimatedCount() int64 {
	return int64(float64(it.ReadCount()) / it.Progress())
}

type Iters []*Iter

func (its Iters) Scan(dest ...interface{}) bool {
	for _, it := range its {
		if it.Scan(dest...) {
			return true
		}
	}
	return false
}

func (its Iters) Close() error {
	var err error
	for _, it := range its {
		if e := it.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (its Iters) Progress() float64 {
	var progress float64
	for _, it := range its {
		progress += it.Progress()
	}
	return progress / float64(len(its))
}

func (its Iters) EstimatedCount() int64 {
	var count int64
	for _, it := range its {
		count += it.EstimatedCount()
	}
	return count
}

func (its Iters) ReadCount() int64 {
	var count int64
	for _, it := range its {
		count += it.ReadCount()
	}
	return count
}

func (its Iters) Finished() bool {
	for _, it := range its {
		if !it.Finished() {
			return false
		}
	}
	return true
}

func (its Iters) Save() error {
	var err error
	for _, it := range its {
		if e := it.Save(); e != nil {
			err = e
		}
	}
	return err
}

func (its Iters) Reset() {
	for _, it := range its {
		it.Reset()
	}
}
