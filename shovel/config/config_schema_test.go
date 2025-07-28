package config

import (
	"strings"
	"testing"

	"github.com/indexsupply/shovel/wpg"
	"kr.dev/diff"
)

func TestDDLWithSchema(t *testing.T) {
	conf := Root{
		Integrations: []Integration{
			{
				Name: "test1",
				Table: wpg.Table{
					Name:   "events",
					Schema: "custom",
					Columns: []wpg.Column{
						{Name: "id", Type: "integer"},
						{Name: "data", Type: "text"},
					},
				},
			},
			{
				Name: "test2",
				Table: wpg.Table{
					Name:   "events",
					Schema: "custom",
					Columns: []wpg.Column{
						{Name: "id", Type: "integer"},
						{Name: "extra", Type: "bytea"},
					},
				},
			},
			{
				Name: "test3",
				Table: wpg.Table{
					Name: "events",
					// No schema specified, should go to public
					Columns: []wpg.Column{
						{Name: "id", Type: "integer"},
						{Name: "value", Type: "numeric"},
					},
				},
			},
		},
	}

	ddl := DDL(conf)

	// Debug: print all DDL statements
	t.Logf("DDL statements generated:")
	for i, stmt := range ddl {
		t.Logf("  [%d]: %s", i, stmt)
	}

	// Should have statements for both custom.events and public.events
	var hasCustomSchema, hasCustomTable, hasPublicTable bool
	for _, stmt := range ddl {
		if stmt == "create schema if not exists custom" {
			hasCustomSchema = true
		}
		if strings.HasPrefix(stmt, "create table if not exists custom.events(") &&
			strings.Contains(stmt, "id integer") &&
			strings.Contains(stmt, "data text") &&
			strings.Contains(stmt, "extra bytea") {
			hasCustomTable = true
		}
		if stmt == "create table if not exists events(id integer, value numeric)" {
			hasPublicTable = true
		}
	}

	diff.Test(t, t.Errorf, true, hasCustomSchema)
	diff.Test(t, t.Errorf, true, hasCustomTable)
	diff.Test(t, t.Errorf, true, hasPublicTable)
}

func TestTableUnionWithSchema(t *testing.T) {
	// Test that tables with the same name but different schemas are not merged
	conf := Root{
		Integrations: []Integration{
			{
				Name: "test1",
				Table: wpg.Table{
					Name:   "events",
					Schema: "schema1",
					Columns: []wpg.Column{
						{Name: "id", Type: "integer"},
					},
				},
			},
			{
				Name: "test2",
				Table: wpg.Table{
					Name:   "events",
					Schema: "schema2",
					Columns: []wpg.Column{
						{Name: "id", Type: "integer"},
					},
				},
			},
		},
	}

	ddl := DDL(conf)

	// Should have separate tables for each schema
	var hasSchema1, hasSchema2, hasSchema1Table, hasSchema2Table bool
	for _, stmt := range ddl {
		if stmt == "create schema if not exists schema1" {
			hasSchema1 = true
		}
		if stmt == "create schema if not exists schema2" {
			hasSchema2 = true
		}
		if stmt == "create table if not exists schema1.events(id integer)" {
			hasSchema1Table = true
		}
		if stmt == "create table if not exists schema2.events(id integer)" {
			hasSchema2Table = true
		}
	}

	diff.Test(t, t.Errorf, true, hasSchema1)
	diff.Test(t, t.Errorf, true, hasSchema2)
	diff.Test(t, t.Errorf, true, hasSchema1Table)
	diff.Test(t, t.Errorf, true, hasSchema2Table)
}