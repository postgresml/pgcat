package pgcat

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

func TestSSL(t *testing.T) {
	t.Cleanup((setupTls(t)))
	t.Run("Prepared Statement and Query on SSL connection works", namedParameterizedPreparedStatementOnSSL)
	t.Run("Connection without ssl params fails", connectionWithoutSSLParams)
}

func namedParameterizedPreparedStatementOnSSL(t *testing.T) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=sharded_db user=sharding_user password=sharding_user sslmode=require", ssl_port))
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

	defer db.Close()

}

func connectionWithoutSSLParams(t *testing.T) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=sharded_db user=sharding_user password=sharding_user sslmode=disable", ssl_port))
	if err != nil {
		t.Fatalf("could not open connection: %+v", err)
	}

	_, err = db.Prepare("SELECT $1")

	if err == nil {
		t.Fatalf("Client connection established without TLS")
	}
}
