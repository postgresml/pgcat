from typing import Tuple
import tempfile
import os
import signal
import time
import tempfile
import subprocess

import psutil
import psycopg2

PGCAT_HOST = "127.0.0.1"
PGCAT_PORT = "6432"


def _pgcat_start(config_path: str):
    pg_cat_send_signal(signal.SIGTERM)
    os.system(f"./target/debug/pgcat {config_path} &")
    time.sleep(2)


def pgcat_start():
    _pgcat_start(config_path='.circleci/pgcat.toml')


def pgcat_generic_start(config: str):
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        f.write(config)
    _pgcat_start(config_path=tmp.name)


def glauth_send_signal(signal: signal.Signals):
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.name() == "glauth":
                os.kill(proc.pid, signal)
    except Exception as e:
        # The process can be gone when we send this signal
        print(e)

    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        time.sleep(2)
        if not os.system('pgrep glauth'):
            raise Exception("glauth not closed after SIGTERM")


def pgcat_start_address_whitelist():
    pg_cat_send_signal(signal.SIGTERM)
    process = subprocess.Popen("./target/debug/pgcat .circleci/pgcat.address_whitelist.toml", shell=True)


def pgcat_start_with_config(config: str):
    config_file = tempfile.NamedTemporaryFile(delete=False)
    config_file.write(str.encode(config))
    config_file.close()
    process = subprocess.Popen(f"./target/debug/pgcat {config_file.name}", shell=True)
    time.sleep(2)
    return process


def pg_cat_send_signal(signal: signal.Signals):
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.name() == "pgcat":
                os.kill(proc.pid, signal)
    except Exception as e:
        # The process can be gone when we send this signal
        print(e)

    if signal == signal.SIGTERM:
        # Returns 0 if pgcat process exists
        time.sleep(2)
        if not os.system('pgrep pgcat'):
            raise Exception("pgcat not closed after SIGTERM")


def connect_db_generic(
    username: str, password: str, host: str, database: str, autocommit: bool = True) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:

    conn = psycopg2.connect(
        f"postgres://{username}:{password}@{host}:{PGCAT_PORT}/{database}?application_name=testing_pgcat",
        connect_timeout=2,
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)


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

def connect_db_trust(
    autocommit: bool = True,
    admin: bool = False,
) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:

    if admin:
        user = "admin_user"
        db = "pgcat"
    else:
        user = "sharding_user"
        db = "sharded_db"

    conn = psycopg2.connect(
        f"postgres://{user}@{PGCAT_HOST}:{PGCAT_PORT}/{db}?application_name=testing_pgcat",
        connect_timeout=2,
    )
    conn.autocommit = autocommit
    cur = conn.cursor()

    return (conn, cur)


def cleanup_conn(conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor):
    cur.close()
    conn.close()
