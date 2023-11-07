package pgcat

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

//go:embed pgcat.toml
var pgcatCfg string

var port = rand.Intn(32760-20000) + 20000

func setup(t *testing.T) func() {
	cfg, err := os.CreateTemp("/tmp", "pgcat_cfg_*.toml")
	if err != nil {
		t.Fatalf("could not create temp file: %+v", err)
	}

	pgcatCfg = strings.Replace(pgcatCfg, "\"${PORT}\"", fmt.Sprintf("%d", port), 1)

	_, err = cfg.Write([]byte(pgcatCfg))
	if err != nil {
		t.Fatalf("could not write temp file: %+v", err)
	}

	commandPath := "../../target/debug/pgcat"
	if os.Getenv("CARGO_TARGET_DIR") != "" {
		commandPath = os.Getenv("CARGO_TARGET_DIR") + "/debug/pgcat"
	}

	cmd := exec.Command(commandPath, cfg.Name())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	go func() {
		err = cmd.Run()
		if err != nil {
			t.Errorf("could not run pgcat: %+v", err)
		}
	}()

	deadline, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancelFunc()
	for {
		select {
		case <-deadline.Done():
			break
		case <-time.After(50 * time.Millisecond):
			db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d database=pgcat user=admin_user password=admin_pass sslmode=disable", port))
			if err != nil {
				continue
			}
			rows, err := db.QueryContext(deadline, "SHOW STATS")
			if err != nil {
				continue
			}
			_ = rows.Close()
			_ = db.Close()
			break
		}
		break
	}

	return func() {
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil {
			t.Fatalf("could not interrupt pgcat: %+v", err)
		}
		err = os.Remove(cfg.Name())
		if err != nil {
			t.Fatalf("could not remove temp file: %+v", err)
		}
	}
}
