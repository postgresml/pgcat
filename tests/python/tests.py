from typing import Tuple
import psycopg2
import psutil
import os
import signal
import subprocess
from threading import Thread
import time

SHUTDOWN_TIMEOUT = 5

PGCAT_HOST = "127.0.0.1"
PGCAT_PORT = "6432"


def pgcat_start():
    pg_cat_send_signal(signal.SIGTERM)
    pgcat_start_command = "./target/debug/pgcat .circleci/pgcat.toml"
    subprocess.Popen(pgcat_start_command.split())


def pg_cat_send_signal(signal: signal.Signals):
    for proc in psutil.process_iter(["pid", "name"]):
        if "pgcat" == proc.name():
            os.kill(proc.pid, signal)
    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        if not os.system('pgrep pgcat'):
            raise Exception("pgcat not closed after SIGTERM")


def connect_normal_db(
    autocommit: bool = False,
) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    conn = psycopg2.connect(
        f"postgres://sharding_user:sharding_user@{PGCAT_HOST}:{PGCAT_PORT}/sharded_db?application_name=testing_pgcat"
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)


def cleanup_conn(conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor):
    cur.close()
    conn.close()
    pg_cat_send_signal(signal.SIGTERM)


def test_normal_db_access():
    conn, cur = connect_normal_db()
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    cleanup_conn(conn, cur)


def test_admin_db_access():
    conn = psycopg2.connect(
        f"postgres://admin_user:admin_pass@{PGCAT_HOST}:{PGCAT_PORT}/pgcat"
    )
    conn.autocommit = True  # BEGIN/COMMIT is not supported by admin db
    cur = conn.cursor()

    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    cleanup_conn(conn, cur)


def test_shutdown_logic():

    ##### NO ACTIVE QUERIES SIGINT HANDLING #####
    # Start pgcat
    server = Thread(target=pgcat_start)
    server.start()

    # Wait for server to fully start up
    time.sleep(2)

    # Create client connection and send query (not in transaction)
    conn, cur = connect_normal_db(True)

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")
    cur.execute("COMMIT;")

    # Send sigint to pgcat
    pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    # Check that any new queries fail after sigint since server should close with no active transactions
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        pass
    else:
        # Fail if query execution succeeded
        raise Exception("Server not closed after sigint")
    cleanup_conn(conn, cur)

    ##### HANDLE TRANSACTION WITH SIGINT #####
    # Start pgcat
    server = Thread(target=pgcat_start)
    server.start()

    # Wait for server to fully start up
    time.sleep(2)

    # Create client connection and begin transaction
    conn, cur = connect_normal_db(True)

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    # Check that any new queries succeed after sigint since server should still allow transaction to complete
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        # Fail if query fails since server closed
        raise Exception("Server closed while in transaction", e.pgerror)

    cleanup_conn(conn, cur)

    ##### HANDLE SHUTDOWN TIMEOUT WITH SIGINT #####
    # Start pgcat
    server = Thread(target=pgcat_start)
    server.start()

    # Wait for server to fully start up
    time.sleep(3)

    # Create client connection and begin transaction, which should prevent server shutdown unless shutdown timeout is reached
    conn, cur = connect_normal_db(True)

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    pg_cat_send_signal(signal.SIGINT)

    # pgcat shutdown timeout is set to SHUTDOWN_TIMEOUT seconds, so we sleep for SHUTDOWN_TIMEOUT + 1 seconds
    time.sleep(SHUTDOWN_TIMEOUT + 1)

    # Check that any new queries succeed after sigint since server should still allow transaction to complete
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        pass
    else:
        # Fail if query execution succeeded
        raise Exception("Server not closed after sigint and expected timeout")

    cleanup_conn(conn, cur)


test_normal_db_access()
test_admin_db_access()
test_shutdown_logic()
