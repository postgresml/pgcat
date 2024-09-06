import signal
import time

import psycopg2

import utils

SHUTDOWN_TIMEOUT = 5

def test_normal_db_access():
    utils.pgcat_start()
    conn, cur = utils.connect_db(autocommit=False)
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)


def test_admin_db_access():
    conn, cur = utils.connect_db(admin=True)

    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)


def test_shutdown_logic():

    # - - - - - - - - - - - - - - - - - -
    # NO ACTIVE QUERIES SIGINT HANDLING

    # Start pgcat
    utils.pgcat_start()

    # Create client connection and send query (not in transaction)
    conn, cur = utils.connect_db()

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")
    cur.execute("COMMIT;")

    # Send sigint to pgcat
    utils.pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    # Check that any new queries fail after sigint since server should close with no active transactions
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        pass
    else:
        # Fail if query execution succeeded
        raise Exception("Server not closed after sigint")

    utils.cleanup_conn(conn, cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # NO ACTIVE QUERIES ADMIN SHUTDOWN COMMAND

    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    conn, cur = utils.connect_db()
    admin_conn, admin_cur = utils.connect_db(admin=True)

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")
    cur.execute("COMMIT;")

    # Send SHUTDOWN command pgcat while not in transaction
    admin_cur.execute("SHUTDOWN;")
    time.sleep(1)

    # Check that any new queries fail after SHUTDOWN command since server should close with no active transactions
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        pass
    else:
        # Fail if query execution succeeded
        raise Exception("Server not closed after sigint")

    utils.cleanup_conn(conn, cur)
    utils.cleanup_conn(admin_conn, admin_cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # HANDLE TRANSACTION WITH SIGINT

    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    conn, cur = utils.connect_db()

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    utils.pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    # Check that any new queries succeed after sigint since server should still allow transaction to complete
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        # Fail if query fails since server closed
        raise Exception("Server closed while in transaction", e.pgerror)

    utils.cleanup_conn(conn, cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # HANDLE TRANSACTION WITH ADMIN SHUTDOWN COMMAND

    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    conn, cur = utils.connect_db()
    admin_conn, admin_cur = utils.connect_db(admin=True)

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")

    # Send SHUTDOWN command pgcat while still in transaction
    admin_cur.execute("SHUTDOWN;")
    if admin_cur.fetchall()[0][0] != "t":
        raise Exception("PgCat unable to send signal")
    time.sleep(1)

    # Check that any new queries succeed after SHUTDOWN command since server should still allow transaction to complete
    try:
        cur.execute("SELECT 1;")
    except psycopg2.OperationalError as e:
        # Fail if query fails since server closed
        raise Exception("Server closed while in transaction", e.pgerror)

    utils.cleanup_conn(conn, cur)
    utils.cleanup_conn(admin_conn, admin_cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # NO NEW NON-ADMIN CONNECTIONS DURING SHUTDOWN
    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    transaction_conn, transaction_cur = utils.connect_db()

    transaction_cur.execute("BEGIN;")
    transaction_cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    utils.pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    start = time.perf_counter()
    try:
        conn, cur = utils.connect_db()
        cur.execute("SELECT 1;")
        utils.cleanup_conn(conn, cur)
    except psycopg2.OperationalError as e:
        time_taken = time.perf_counter() - start
        if time_taken > 0.1:
            raise Exception(
                "Failed to reject connection within 0.1 seconds, got", time_taken, "seconds")
        pass
    else:
        raise Exception("Able connect to database during shutdown")

    utils.cleanup_conn(transaction_conn, transaction_cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # ALLOW NEW ADMIN CONNECTIONS DURING SHUTDOWN
    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    transaction_conn, transaction_cur = utils.connect_db()

    transaction_cur.execute("BEGIN;")
    transaction_cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    utils.pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    try:
        conn, cur = utils.connect_db(admin=True)
        cur.execute("SHOW DATABASES;")
        utils.cleanup_conn(conn, cur)
    except psycopg2.OperationalError as e:
        raise Exception(e)

    utils.cleanup_conn(transaction_conn, transaction_cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # ADMIN CONNECTIONS CONTINUING TO WORK AFTER SHUTDOWN
    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction
    transaction_conn, transaction_cur = utils.connect_db()
    transaction_cur.execute("BEGIN;")
    transaction_cur.execute("SELECT 1;")

    admin_conn, admin_cur = utils.connect_db(admin=True)
    admin_cur.execute("SHOW DATABASES;")

    # Send sigint to pgcat while still in transaction
    utils.pg_cat_send_signal(signal.SIGINT)
    time.sleep(1)

    try:
        admin_cur.execute("SHOW DATABASES;")
    except psycopg2.OperationalError as e:
        raise Exception("Could not execute admin command:", e)

    utils.cleanup_conn(transaction_conn, transaction_cur)
    utils.cleanup_conn(admin_conn, admin_cur)
    utils.pg_cat_send_signal(signal.SIGTERM)

    # - - - - - - - - - - - - - - - - - -
    # HANDLE SHUTDOWN TIMEOUT WITH SIGINT

    # Start pgcat
    utils.pgcat_start()

    # Create client connection and begin transaction, which should prevent server shutdown unless shutdown timeout is reached
    conn, cur = utils.connect_db()

    cur.execute("BEGIN;")
    cur.execute("SELECT 1;")

    # Send sigint to pgcat while still in transaction
    utils.pg_cat_send_signal(signal.SIGINT)

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

    utils.cleanup_conn(conn, cur)
    utils.pg_cat_send_signal(signal.SIGTERM)
