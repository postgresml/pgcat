from typing import Tuple
import os
import psutil
import signal
import time

import psycopg2

PGCAT_HOST = "127.0.0.1"
PGCAT_PORT = "6432"

def pgcat_start():
    pg_cat_send_signal(signal.SIGTERM)
    os.system("./target/debug/pgcat .circleci/pgcat.toml &")
    time.sleep(2)


def pg_cat_send_signal(signal: signal.Signals):
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if "pgcat" == proc.name():
                os.kill(proc.pid, signal)
    except Exception as e:
        # The process can be gone when we send this signal
        print(e)

    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        time.sleep(2)
        if not os.system('pgrep pgcat'):
            raise Exception("pgcat not closed after SIGTERM")


def connect_db(
    autocommit: bool = True,
    admin: bool = False,
) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:

    if admin:
        user = "admin_user"
        password = "admin_pass"
        db = "pgcat"
    else:
        user = "sharding_user"
        password = "sharding_user"
        db = "sharded_db"

    conn = psycopg2.connect(
        f"postgres://{user}:{password}@{PGCAT_HOST}:{PGCAT_PORT}/{db}?application_name=testing_pgcat",
        connect_timeout=2,
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)


def cleanup_conn(conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor):
    cur.close()
    conn.close()
