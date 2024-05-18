package casscanner

import (
	"fmt"
	"strings"
	"text/scanner"
)

// parsedQuery is a representation of a CQL query
// SELECT [columnsPart] FROM [keyspace].[table] WHERE [lastPart]

type parsedQuery struct {
	columnsPart string

	keyspace string
	table    string

	hasWhere bool

	lastPart string
}

func (pq *parsedQuery) addWhere(whereClause string) {
	if pq.hasWhere {
		pq.lastPart = whereClause + " AND " + pq.lastPart
	} else {
		pq.lastPart = whereClause + " " + pq.lastPart
		pq.hasWhere = true
	}
}

func (pq *parsedQuery) String() string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(pq.columnsPart)
	sb.WriteString(" FROM ")
	sb.WriteString(pq.keyspace)
	sb.WriteString(".")
	sb.WriteString(pq.table)

	if pq.hasWhere {
		sb.WriteString(" WHERE ")
	}

	sb.WriteString(pq.lastPart)

	return sb.String()
}

func parceCQLQuery(stmt string) (*parsedQuery, error) {
	var q parsedQuery

	var s scanner.Scanner
	s.Init(strings.NewReader(stmt))

	token := s.Scan()
	if token != scanner.Ident {
		if !strings.EqualFold(s.TokenText(), "select") {
			return nil, fmt.Errorf("query should start with SELECT: %s", stmt)
		}
	}

	for {
		if s.Scan() == scanner.EOF {
			return nil, fmt.Errorf("invalid statement, should contain FROM: %s", stmt)
		}

		if strings.EqualFold(s.TokenText(), "FROM") {
			break
		}
	}
	q.columnsPart = stmt[len("select ") : s.Pos().Offset-len(s.TokenText())-1]

	if s.Scan() != scanner.Ident {
		return nil, fmt.Errorf("invalid statement, should contain keyspace after from")
	}
	q.keyspace = s.TokenText()

	s.Scan()
	if s.TokenText() != "." {
		return nil, fmt.Errorf("invalid statement, should contain keyspace.table: %s", stmt)
	}

	if s.Scan() != scanner.Ident {
		return nil, fmt.Errorf("invalid statement, should contain table after keyspace")
	}
	q.table = s.TokenText()

	s.Scan()
	if strings.EqualFold(s.TokenText(), "WHERE") {
		q.hasWhere = true

		if s.Scan() == scanner.EOF {
			return &q, fmt.Errorf("invalid statement, should contain something after WHERE: %s", stmt)
		}

	} else {
		if s.Scan() == scanner.EOF {
			return &q, nil
		}
	}

	q.lastPart = stmt[s.Pos().Offset-len(s.TokenText()):]

	return &q, nil
}
