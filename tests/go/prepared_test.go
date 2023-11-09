package pgcat

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"testing"
)

func Test(t *testing.T) {
	t.Cleanup(setup(t))
	t.Run("Named parameterized prepared statement works", namedParameterizedPreparedStatement)
	t.Run("Unnamed parameterized prepared statement works", unnamedParameterizedPreparedStatement)
}

func namedParameterizedPreparedStatement(t *testing.T) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=sharded_db user=sharding_user password=sharding_user sslmode=disable", port))
	if err != nil {
		t.Fatalf("could not open connection: %+v", err)
	}

	stmt, err := db.Prepare("SELECT $1")

	if err != nil {
		t.Fatalf("could not prepare: %+v", err)
	}

	for i := 0; i < 100; i++ {
		rows, err := stmt.Query(1)
		if err != nil {
			t.Fatalf("could not query: %+v", err)
		}
		_ = rows.Close()
	}
}

func unnamedParameterizedPreparedStatement(t *testing.T) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=sharded_db user=sharding_user password=sharding_user sslmode=disable", port))
	if err != nil {
		t.Fatalf("could not open connection: %+v", err)
	}

	for i := 0; i < 100; i++ {
		// Under the hood QueryContext generates an unnamed parameterized prepared statement
		rows, err := db.QueryContext(context.Background(), "SELECT $1", 1)
		if err != nil {
			t.Fatalf("could not query: %+v", err)
		}
		_ = rows.Close()
	}
}
