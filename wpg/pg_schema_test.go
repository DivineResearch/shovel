package wpg

import (
	"context"
	"testing"

	"blake.io/pqx/pqxtest"
	"github.com/jackc/pgx/v5/pgxpool"
	"kr.dev/diff"
)

func TestTableWithSchema(t *testing.T) {
	ctx := context.Background()
	pqxtest.CreateDB(t, "")
	pg, err := pgxpool.New(ctx, pqxtest.DSNForTest(t))
	diff.Test(t, t.Fatalf, nil, err)

	table := Table{
		Name:   "test_table",
		Schema: "test_schema",
		Columns: []Column{
			{Name: "id", Type: "integer"},
			{Name: "name", Type: "text"},
		},
	}

	// Test QualifiedName
	diff.Test(t, t.Errorf, "test_schema.test_table", table.QualifiedName())

	// Test DDL generation
	ddl := table.DDL()
	diff.Test(t, t.Errorf, 3, len(ddl))
	diff.Test(t, t.Errorf, "create schema if not exists test_schema", ddl[0])
	diff.Test(t, t.Errorf, "create table if not exists test_schema.test_table(id integer, name text)", ddl[1])

	// Test migration
	err = table.Migrate(ctx, pg)
	diff.Test(t, t.Fatalf, nil, err)

	// Verify table exists in correct schema
	var exists bool
	err = pg.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 
			AND table_name = $2
		)`, "test_schema", "test_table").Scan(&exists)
	diff.Test(t, t.Fatalf, nil, err)
	diff.Test(t, t.Errorf, true, exists)

	// Test adding a column
	table.Columns = append(table.Columns, Column{Name: "age", Type: "integer"})
	err = table.Migrate(ctx, pg)
	diff.Test(t, t.Fatalf, nil, err)

	// Verify column was added
	var colCount int
	err = pg.QueryRow(ctx, `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_schema = $1 
		AND table_name = $2
	`, "test_schema", "test_table").Scan(&colCount)
	diff.Test(t, t.Fatalf, nil, err)
	diff.Test(t, t.Errorf, 3, colCount)

	// Clean up
	_, err = pg.Exec(ctx, "drop schema test_schema cascade")
	diff.Test(t, t.Errorf, nil, err)
}

func TestTableWithoutSchema(t *testing.T) {
	table := Table{
		Name: "test_table",
		Columns: []Column{
			{Name: "id", Type: "integer"},
		},
	}

	// Test QualifiedName returns just the name when no schema
	diff.Test(t, t.Errorf, "test_table", table.QualifiedName())

	// Test DDL generation without schema
	ddl := table.DDL()
	diff.Test(t, t.Errorf, 1, len(ddl))
	diff.Test(t, t.Errorf, "create table if not exists test_table(id integer)", ddl[0])
}