import psycopg2
import pytest
import time
import utils


def setup_module(module) -> None:
    pgcat_conf = """
    [general]

    host = "0.0.0.0"
    port = 6433
    admin_username = "pgcat"
    admin_password = "pgcat"
    max_clients = 4

    [pools.pgml.users.0]
    username = "limited_user"
    password = "limited_user"
    server_username = "sharding_user"
    pool_size = 10
    max_clients = 2
    min_pool_size = 1
    pool_mode = "transaction"

    [pools.pgml.users.1]
    username = "unlimited_user"
    password = "unlimited_user"
    server_username = "sharding_user"
    pool_size = 10
    min_pool_size = 1
    pool_mode = "transaction"

    [pools.pgml.shards.0]
    servers = [
      ["127.0.0.1", 5432, "primary"]
    ]
    database = "some_db"
    """
    module.pgcat_process = utils.pgcat_generic_start_from_string(pgcat_conf)
    time.sleep(2)


def teardown_module(module) -> None:
    module.pgcat_process.terminate()
    module.pgcat_process.wait()
    time.sleep(2)


def test_pgcat_limit() -> None:
    # Open 4 connections (2 for each user)
    conns = [
        utils.connect_db_generic(
          username=user, password=user, host='127.0.0.1', database='pgml', port=6433)
        for user in ['unlimited_user', 'unlimited_user'] * 2]

    # Verify 5th connection does not work for both users
    with pytest.raises(psycopg2.OperationalError):
      utils.connect_db_generic(
        username='limited_user', password='limited_user', host='127.0.0.1', database='pgml', port=6433)

    with pytest.raises(psycopg2.OperationalError):
      utils.connect_db_generic(
        username='unlimited_user', password='unlimited_user', host='127.0.0.1', database='pgml', port=6433)

    # Close 4th connection
    (conn, curr) = conns.pop(-1)
    utils.cleanup_conn(conn, curr)

    utils.connect_db_generic(
        username='unlimited_user', password='unlimited_user', host='127.0.0.1', database='pgml', port=6433)


def test_user_limit() -> None:
  # Open 2 connections for limited User
    limited_conns = [
        utils.connect_db_generic(
          username='limited_user', password='limited_user', host='127.0.0.1', database='pgml', port=6433)
        for _ in range(2)]

  # Validate 3rd connection does not work
    with pytest.raises(psycopg2.OperationalError):
        utils.connect_db_generic(
            username='limited_user', password='limited_user', host='127.0.0.1', database='pgml', port=6433)

  # Validate unlimited user can still open connection
    utils.connect_db_generic(
        username='unlimited_user', password='unlimited_user', host='127.0.0.1', database='pgml', port=6433)

  # Close 2nd Connection
    (conn, curr) = limited_conns.pop(-1)
    utils.cleanup_conn(conn, curr)

    utils.connect_db_generic(
        username='limited_user', password='limited_user', host='127.0.0.1', database='pgml', port=6433)
