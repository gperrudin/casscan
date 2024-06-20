package casscanner

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"math"
	"math/big"
	"strconv"
	"strings"
)

type Scanner struct {
	config     Config
	stateStore scanStateStore
	session    *gocql.Session
}

type query struct {
	stmt   string
	values []interface{}
	rng    tokenRange
}

func NewScanner(stateStore Store, session *gocql.Session, options ...Option) *Scanner {
	s := &Scanner{
		stateStore: newScanStateStore(stateStore),
		session:    session,
	}

	for _, opt := range options {
		opt(&s.config)
	}

	return s
}

// Iter creates a new iterator for the given scanId and query. The query should be a SELECT statement.
func (s *Scanner) Iter(ctx context.Context, scanId string, stmt string, values ...interface{}) (*Iter, error) {
	q := query{
		stmt:   stmt,
		values: values,
		rng: tokenRange{
			from: nil,
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
	state, err := s.stateStore.load(ctx, scanId)
	if err != nil {
		return nil, err
	}

	gocqlQuery, err := s.buildQuery(ctx, q, state)
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

	if state != nil {
		it.state = *state
	}

	return &it, nil
}

func (s *Scanner) buildQuery(ctx context.Context, q query, state *scanState) (*gocql.Query, error) {
	parsed, err := parceCQLQuery(q.stmt)
	if err != nil {
		return nil, fmt.Errorf("could not parse query: %w", err)
	}

	pkColumns, ckColumns, err := getColumns(s.session, parsed.keyspace, parsed.table)
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

	if state != nil && state.Token != nil {
		var lastTokenClause string
		if len(ckColumns) > 0 {
			// if there is a clustering key, we need to start from the last token because we don't know if there are more rows within this partition
			lastTokenClause = fmt.Sprintf("token(%s) >= %d", strings.Join(pkColumns, ", "), *state.Token)
		} else {
			lastTokenClause = fmt.Sprintf("token(%s) > %d", strings.Join(pkColumns, ", "), *state.Token)
		}

		parsed.addWhere(lastTokenClause)
	}

	parsed.columnsPart += fmt.Sprintf(", token(%s)", strings.Join(pkColumns, ", "))

	return s.session.Query(parsed.String(), q.values...).WithContext(ctx), nil
}

// getPrimaryKeyColumns returns the pk and ck columns for the given keyspace and table.
func getColumns(s *gocql.Session, keyspace, table string) ([]string, []string, error) {
	keyspaceMetadata, err := s.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, nil, err
	}
	tableMetadata := keyspaceMetadata.Tables[table]
	if tableMetadata == nil {
		return nil, nil, fmt.Errorf("could not find metadata for table: %s.%s", keyspace, table)
	}

	pkColumns := make([]string, 0)
	for _, pkColumn := range tableMetadata.PartitionKey {
		pkColumns = append(pkColumns, pkColumn.Name)
	}

	ckColumns := make([]string, 0)
	for _, ckColumn := range tableMetadata.ClusteringColumns {
		ckColumns = append(ckColumns, ckColumn.Name)
	}

	return pkColumns, ckColumns, nil
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

	lastSavedCount int64
}

// Scan is a wrapper around gocql.Iter.Scan
func (it *Iter) Scan(dest ...interface{}) bool {
	defer it.autoSave()

	if it.state.Finished {
		return false
	}

	values := append(dest, &it.state.Token)
	if it.iter.Scan(values...) {
		it.state.ScanRowsCount++
		return true
	} else if err := it.iter.Close(); err != nil {
		return false
	} else {
		it.state.Finished = true
		return false
	}
}

// Save stores the current state of the iterator.
func (it *Iter) Save() error {
	return it.doSave()
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
	if it.state.Token == nil {
		return 0
	}

	rng := tokenRange{
		from: it.query.rng.from,
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

	if *it.state.Token < *rng.from {
		return 0
	}
	if *it.state.Token >= *rng.to {
		return 1
	}

	// progress = (state.Token - rng.from) / (rng.to - rng.from)
	// we need to use big int/floats to avoid overflow

	rngSize := big.NewInt(*rng.to)
	rngSize = rngSize.Sub(rngSize, big.NewInt(*rng.from))

	doneSize := big.NewInt(*it.state.Token)
	doneSize.Sub(doneSize, big.NewInt(*rng.from))

	progress, _ := new(big.Float).Quo(new(big.Float).SetInt(doneSize), new(big.Float).SetInt(rngSize)).Float64()
	return progress
}

// EstimatedCount returns the estimated number of rows that the iterator will read.
func (it *Iter) EstimatedCount() int64 {
	return int64(float64(it.ReadCount()) / it.Progress())
}

func (it *Iter) autoSave() error {
	if it.scanner.config.AutoSaveInterval <= 0 {
		return nil
	}

	if it.Finished() && (it.state.ScanRowsCount-it.lastSavedCount >= it.scanner.config.AutoSaveInterval) {
		if err := it.doSave(); err != nil {
			return nil
		}
		it.lastSavedCount = it.state.ScanRowsCount
	}
	return nil
}

func (it *Iter) doSave() error {
	return it.scanner.store(it.ctx, it.scanId, &it.state)
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
