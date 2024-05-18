package casscanner

import (
	"cmp"
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func TestSplitScan(t *testing.T) {
	var (
		ctx          = context.Background()
		store        = NewMemoryStore()
		session      = getSession(t)
		insertedRows []Row
	)

	for i := 0; i < 100; i++ {
		insertedRows = append(insertedRows, Row{
			key:   "key_" + strconv.Itoa(i),
			value: "value_" + strconv.Itoa(i),
		})
	}

	bootStrap(t, insertedRows)

	scanner := NewScanner(store, session)
	iters, err := scanner.SplitIter(ctx, "test_scan", 8, "SELECT id, value FROM tablescan.tablescan_v2_test")
	require.Nil(t, err)

	// first read 10 per iter
	var lock sync.Mutex
	rows := make([][]Row, len(iters))

	var eg errgroup.Group
	for i, it := range iters {
		i := i
		it := it
		eg.Go(func() error {
			defer it.Save()

			lock.Lock()
			defer lock.Unlock()

			var row Row
			for j := 0; j < 10; j++ {
				if !it.Scan(&row.key, &row.value) {
					return nil
				}
				rows[i] = append(rows[i], row)
			}
			return nil
		})
	}
	require.Nil(t, eg.Wait())

	// then read until finished
	for i, it := range iters {
		i := i
		it := it
		eg.Go(func() error {
			defer it.Save()

			var row Row
			for it.Scan(&row.key, &row.value) {
				fmt.Printf("%d: %v\n", i, row)
				rows[i] = append(rows[i], row)
			}
			return nil
		})
	}
	require.Nil(t, eg.Wait())

	// merge
	var merged []Row
	for _, r := range rows {
		merged = append(merged, r...)
	}

	RequireSameRows(t, insertedRows, merged)
}

func TestSave(t *testing.T) {
	var (
		ctx          = context.Background()
		store        = NewMemoryStore()
		session      = getSession(t)
		row          Row
		rows         []Row
		insertedRows = []Row{
			{"a", "1"},
			{"b", "2"},
		}
	)

	bootStrap(t, insertedRows)

	{
		// read first row

		scanner := NewScanner(store, session)
		it, err := scanner.Iter(ctx, "test_scan", "SELECT id, value FROM tablescan.tablescan_v2_test")
		require.Nil(t, err)
		require.False(t, it.Finished())
		require.Equal(t, float64(0), it.Progress())

		require.True(t, it.Scan(&row.key, &row.value))

		require.Nil(t, it.Close())
		require.Nil(t, it.Save())
		rows = append(rows, row)
	}

	{
		// read second row

		scanner := NewScanner(store, session)
		it, err := scanner.Iter(ctx, "test_scan", "SELECT id, value FROM tablescan.tablescan_v2_test")
		require.Nil(t, err)

		require.True(t, it.Scan(&row.key, &row.value))
		require.Nil(t, it.Save())
		rows = append(rows, row)
		require.False(t, it.Scan(&row.key, &row.value))
		require.True(t, it.Finished())
		require.True(t, it.Progress() == 1)
		require.Nil(t, it.Save())
	}

	{
		scanner := NewScanner(store, session)
		it, err := scanner.Iter(ctx, "test_scan", "SELECT * FROM tablescan.tablescan_v2_test")
		require.True(t, it.Finished())
		require.True(t, it.Progress() == 1)
		require.Nil(t, err)
		require.Nil(t, it.Save())

		require.False(t, it.Scan(&row.key, &row.value))
	}

	RequireSameRows(t, insertedRows, rows)
}

type Row struct {
	key, value string
}

func RequireSameRows(t *testing.T, rows1, rows2 []Row) {
	if len(rows1) != len(rows2) {
		t.Fatalf("different len (%d != %d) expected %v, got %v", len(rows1), len(rows2), rows1, rows2)
	}

	sortFn := func(i Row, j Row) int {
		res := cmp.Compare(i.key, j.key)
		if res != 0 {
			return res
		}
		return cmp.Compare(i.value, j.value)
	}
	slices.SortFunc(rows1, sortFn)
	slices.SortFunc(rows2, sortFn)

	for i := range rows1 {
		if rows1[i] != rows2[i] {
			t.Fatalf("expected %v, got %v", rows1, rows2)
		}
	}
}

func bootStrap(t *testing.T, rows []Row) {
	session := getSession(t)

	mustExec(t, session.Query(`CREATE KEYSPACE IF NOT EXISTS tablescan WITH REPLICATION = {'class' : 'SimpleStrategy'}`))
	mustExec(t, session.Query(`
		CREATE TABLE IF NOT EXISTS tablescan.tablescan_v2_test (
		id TEXT PRIMARY KEY,
		value TEXT
		)
	`))
	mustExec(t, session.Query(`TRUNCATE TABLE tablescan.tablescan_v2_test`))

	var eg errgroup.Group
	eg.SetLimit(20)
	for _, row := range rows {
		row := row
		eg.Go(func() error {
			return session.Query("INSERT INTO tablescan.tablescan_v2_test (id, value) values (?, ?)", row.key, row.value).Exec()
		})
	}
	require.Nil(t, eg.Wait())
}

func getSession(t *testing.T) *gocql.Session {
	cluster := gocql.NewCluster("localhost")

	s, err := cluster.CreateSession()
	require.Nil(t, err)

	s.SetPageSize(1)

	return s
}

func mustExec(t *testing.T, q *gocql.Query) {
	require.Nil(t, q.Exec())
}
