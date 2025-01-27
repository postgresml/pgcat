package pgcat

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/lib/pq"
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
	var wg sync.WaitGroup
	errCh := make(chan error, 2) // create error channel

	// Have two concurrent clients executing different unnamed prepared statements
	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=sharded_db user=sharding_user password=sharding_user sslmode=disable", port))
			if err != nil {
				errCh <- err // send error to channel
				return
			}

			for j := 0; j < 100; j++ {

				// Under the hood QueryContext generates an unnamed parameterized prepared statement
				switch id {
				case 0:
					rows, err := db.QueryContext(context.Background(), "SELECT $1", 1)
					if err != nil {
						errCh <- err // send error to channel
						return
					}
					_ = rows.Close()

				case 1:
					rows, err := db.QueryContext(context.Background(), "SELECT $1, $2", 1, 2)
					if err != nil {
						errCh <- err // send error to channel
						return
					}
					_ = rows.Close()
				}

			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("received error from goroutine: %v", err)
		}
	}
}
