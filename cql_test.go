package casscanner

import (
	"reflect"
	"testing"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		want    *parsedQuery
		wantErr bool
	}{
		{
			name: "2 columns, no WHERE",
			stmt: "SELECT col1, col2 FROM tablescan.tablescan_v2_test",
			want: &parsedQuery{
				columnsPart: "col1, col2",
				keyspace:    "tablescan",
				table:       "tablescan_v2_test",
				lastPart:    "",
				hasWhere:    false,
			},
		},
		{
			name: "1 column, with WHERE",
			stmt: "SELECT col1 FROM tablescan.tablescan_v2_test WHERE col1 > 12",
			want: &parsedQuery{
				columnsPart: "col1",
				keyspace:    "tablescan",
				table:       "tablescan_v2_test",
				lastPart:    "col1 > 12",
				hasWhere:    true,
			},
		},
		{
			name: "1 column, with WHERE",
			stmt: "SELECT col1 FROM tablescan.tablescan_v2_test WHERE col1 > 12 LIMIT 12",
			want: &parsedQuery{
				columnsPart: "col1",
				keyspace:    "tablescan",
				table:       "tablescan_v2_test",
				lastPart:    "col1 > 12 LIMIT 12",
				hasWhere:    true,
			},
		},
		{
			name:    "no FROM",
			stmt:    "SELCT col1 FRM tablescan.tablescan_v2_test WHERE col1 > 12",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parceCQLQuery(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("parceCQLQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parceCQLQuery() got = %v, want %v", got, tt.want)
			}
		})
	}
}
