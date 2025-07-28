package wpg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"blake.io/pqx/pqxtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:generate go run gen/main.go

func TestPG(tb testing.TB, schema string) *pgxpool.Pool {
	tb.Helper()
	db := pqxtest.CreateDB(tb, schema)

	var name string
	const q = "select current_database()"
	err := db.QueryRow(q).Scan(&name)
	if err != nil {
		tb.Fatal(err)
	}

	cfg, err := pgconn.ParseConfig(pqxtest.DSN())
	if err != nil {
		tb.Fatal(err)
	}

	pgurl := fmt.Sprintf("postgres://localhost:%d/%s", cfg.Port, name)
	pg, err := pgxpool.New(context.Background(), pgurl)
	if err != nil {
		tb.Fatal(err)
	}
	return pg
}

type Column struct {
	Name string `db:"column_name"json:"name"`
	Type string `db:"data_type"json:"type"`
}

func quote(s string) string {
	if _, ok := reservedWords[strings.ToLower(s)]; ok {
		return strconv.Quote(s)
	}
	return s
}

type Table struct {
	Name    string   `json:"name"`
	Schema  string   `json:"schema"`
	Columns []Column `json:"columns"`

	DisableUnique bool       `json:"disable_unique"`
	Unique        [][]string `json:"unique"`
	Index         [][]string `json:"index"`
}

// QualifiedName returns the table name with schema prefix if schema is specified
func (t Table) QualifiedName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

func (t Table) DDL() []string {
	if len(t.Columns) == 0 {
		return nil
	}
	var res []string

	// Create schema if specified
	if t.Schema != "" {
		res = append(res, fmt.Sprintf("create schema if not exists %s", t.Schema))
	}

	tableName := t.QualifiedName()

	createTable := fmt.Sprintf("create table if not exists %s(", tableName)
	for i, col := range t.Columns {
		createTable += fmt.Sprintf("%s %s", quote(col.Name), col.Type)
		if i+1 == len(t.Columns) {
			createTable += ")"
			break
		}
		createTable += ", "
	}
	res = append(res, createTable)

	for _, cols := range t.Unique {
		createIndex := fmt.Sprintf(
			"create unique index if not exists u_%s on %s (",
			t.Name,
			tableName,
		)
		for i, cname := range cols {
			createIndex += quote(cname)
			if i+1 == len(cols) {
				createIndex += ")"
				break
			}
			createIndex += ", "
		}
		res = append(res, createIndex)
	}

	for _, cols := range t.Index {
		var indexName string
		for i := range cols {
			indexName += strings.ReplaceAll(cols[i], " ", "_")
			if i+1 != len(cols) {
				indexName += "_"
			}
		}
		createIndex := fmt.Sprintf(
			"create index if not exists shovel_%s on %s (",
			indexName,
			tableName,
		)
		for i, cname := range cols {
			createIndex += quote(cname)
			if i+1 == len(cols) {
				createIndex += ")"
				break
			}
			createIndex += ", "
		}
		res = append(res, createIndex)
	}

	return res
}

func (t Table) Migrate(ctx context.Context, pg Conn) error {
	for _, stmt := range t.DDL() {
		if _, err := pg.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("table %q stmt %q: %w", t.QualifiedName(), stmt, err)
		}
	}
	diff, err := Diff(ctx, pg, t.Name, t.Columns, t.Schema)
	if err != nil {
		return fmt.Errorf("getting diff for %s: %w", t.QualifiedName(), err)
	}
	for _, c := range diff.Add {
		var q = fmt.Sprintf(
			"alter table %s add column if not exists %s %s",
			t.QualifiedName(),
			quote(c.Name),
			c.Type,
		)
		if _, err := pg.Exec(ctx, q); err != nil {
			return fmt.Errorf("adding column %s/%s: %w", t.QualifiedName(), c.Name, err)
		}
	}
	return nil
}

type DiffDetails struct {
	Remove []Column
	Add    []Column
}

func Diff(
	ctx context.Context,
	pg Conn,
	tableName string,
	cols []Column,
	schema string,
) (DiffDetails, error) {
	// Default to public schema if not specified
	if schema == "" {
		schema = "public"
	}
	const q = `
		select column_name, data_type
		from information_schema.columns
		where table_schema = $1
		and table_name = $2
	`
	rows, _ := pg.Query(ctx, q, schema, tableName)
	indb, err := pgx.CollectRows(rows, pgx.RowToStructByName[Column])
	if err != nil {
		return DiffDetails{}, fmt.Errorf("querying for table info: %w", err)
	}
	var dd DiffDetails
	for i := range cols {
		var found bool
		for j := range indb {
			if cols[i].Name == indb[j].Name {
				found = true
				break
			}
		}
		if !found {
			dd.Add = append(dd.Add, cols[i])
		}
	}
	for i := range indb {
		var found bool
		for j := range cols {
			if indb[i].Name == cols[j].Name {
				found = true
				break
			}
		}
		if !found {
			dd.Remove = append(dd.Remove, indb[i])
		}
	}
	return dd, nil
}

func Indexes(ctx context.Context, pg Conn, table string) []map[string]any {
	const q = `
		select indexname, indexdef
		from pg_indexes
		where tablename = $1
	`
	rows, _ := pg.Query(ctx, q, table)
	res, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return []map[string]any{map[string]any{"error": err.Error()}}
	}
	return res
}

func RowEstimate(ctx context.Context, pg Conn, table string) string {
	const q = `
		select trim(to_char(reltuples, '999,999,999,999'))
		from pg_class
		where relname = $1
	`
	var res string
	if err := pg.QueryRow(ctx, q, table).Scan(&res); err != nil {
		return err.Error()
	}
	switch {
	case res == "0":
		return "pending"
	case strings.HasPrefix(res, "-"):
		return "pending"
	default:
		return res
	}
}

func TableSize(ctx context.Context, pg Conn, table string) string {
	const q = `SELECT pg_size_pretty(pg_total_relation_size($1))`
	var res string
	if err := pg.QueryRow(ctx, q, table).Scan(&res); err != nil {
		return err.Error()
	}
	return res
}
