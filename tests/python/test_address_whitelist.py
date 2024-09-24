import socket
import time
import utils

import psycopg2
import pytest

PRIVATE_IP_ADDRESS = socket.gethostbyname(socket.gethostname())

# TODO Test with admin user as well
config_file = """
[general]

host = "0.0.0.0"
port = 6432
admin_username = "pgcat"
admin_password = "pgcat"

[pools.pgml.users.0]
username = "ip_whitelist"
password = "ip_whitelist"
server_username = "postgres"
address_whitelist = [
  "127.0.0.1",
]
pool_size = 10

[pools.pgml.users.1]
username = "hostname_whitelist"
password = "hostname_whitelist"
server_username = "postgres"
address_whitelist = [
  "localhost"
]
pool_size = 10

[pools.pgml.users.2]
username = "private_ip_range_whitelist"
password = "private_ip_range_whitelist"
server_username = "postgres"
address_whitelist = [
  "127.0.0.1/32"
]
pool_size = 10

[pools.pgml.users.3]
username = "public_ip_range_whitelist"
password = "public_ip_range_whitelist"
server_username = "postgres"
address_whitelist = [
  "0.0.0.0/0"
]
pool_size = 10

[pools.pgml.shards.0]
servers = [
  ["127.0.0.1", 9876, "primary"]
]
database = "postgres"
"""


class TestAdminUserPrivateIpRange:
  @classmethod
  def setup_class(cls):
    config_file = """
    [general]

    host = "0.0.0.0"
    port = 6432
    admin_username = "pgcat"
    admin_password = "pgcat"
    admin_address_whitelist = [
      "127.0.0.1/32"
    ]

    [pools.pgml.users.1]
    username = "hostname_whitelist"
    password = "hostname_whitelist"
    server_username = "postgres"
    address_whitelist = [
      "localhost"
    ]
    pool_size = 10

    [pools.pgml.shards.0]
    servers = [
      ["127.0.0.1", 9876, "primary"]
    ]
    database = "postgres"
    """
    cls.pgcat_process = utils.pgcat_start_with_config(config_file)
    time.sleep(1)

  @classmethod
  def teardown_class(cls):
    cls.pgcat_process.kill()
    cls.pgcat_process.wait()
    time.sleep(1)

  def test_positive(self) -> None:
    conn, cur = utils.connect_db_generic(
        username='pgcat', password='pgcat', host='127.0.0.1', database='pgcat')
    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_negative(self) -> None:
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(username='pgcat', password='pgcat', host=PRIVATE_IP_ADDRESS, database='pgcat')


class TestAdminUserPublicIpRange:
  @classmethod
  def setup_class(cls):
    config_file = """
    [general]

    host = "0.0.0.0"
    port = 6432
    admin_username = "pgcat"
    admin_password = "pgcat"
    admin_address_whitelist = [
      "0.0.0.0/0"
    ]

    [pools.pgml.users.1]
    username = "hostname_whitelist"
    password = "hostname_whitelist"
    server_username = "postgres"
    address_whitelist = [
      "localhost"
    ]
    pool_size = 10

    [pools.pgml.shards.0]
    servers = [
      ["127.0.0.1", 9876, "primary"]
    ]
    database = "postgres"
    """
    cls.pgcat_process = utils.pgcat_start_with_config(config_file)
    time.sleep(1)

  @classmethod
  def teardown_class(cls):
    cls.pgcat_process.kill()
    cls.pgcat_process.wait()
    time.sleep(1)

  def test_positive(self) -> None:
    conn, cur = utils.connect_db_generic(
        username='pgcat', password='pgcat', host='127.0.0.1', database='pgcat')
    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_negative(self) -> None:
    conn, cur = utils.connect_db_generic(
        username='pgcat', password='pgcat', host=PRIVATE_IP_ADDRESS, database='pgcat')
    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

class TestAdminUserIP:
  @classmethod
  def setup_class(cls):
    config_file = """
    [general]

    host = "0.0.0.0"
    port = 6432
    admin_username = "pgcat"
    admin_password = "pgcat"
    admin_address_whitelist = [
      "127.0.0.1"
    ]

    [pools.pgml.users.1]
    username = "hostname_whitelist"
    password = "hostname_whitelist"
    server_username = "postgres"
    address_whitelist = [
      "localhost"
    ]
    pool_size = 10

    [pools.pgml.shards.0]
    servers = [
      ["127.0.0.1", 9876, "primary"]
    ]
    database = "postgres"
    """
    cls.pgcat_process = utils.pgcat_start_with_config(config_file)
    time.sleep(1)

  @classmethod
  def teardown_class(cls):
    cls.pgcat_process.kill()
    cls.pgcat_process.wait()
    time.sleep(1)

  def test_positive(self) -> None:
    conn, cur = utils.connect_db_generic(
        username='pgcat', password='pgcat', host='127.0.0.1', database='pgcat')
    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_negative(self) -> None:
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(username='pgcat', password='pgcat', host=PRIVATE_IP_ADDRESS, database='pgcat')


class TestAdminUserPublicHostname:
  @classmethod
  def setup_class(cls):
    config_file = """
    [general]

    host = "0.0.0.0"
    port = 6432
    admin_username = "pgcat"
    admin_password = "pgcat"
    admin_address_whitelist = [
      "localhost"
    ]

    [pools.pgml.users.1]
    username = "hostname_whitelist"
    password = "hostname_whitelist"
    server_username = "postgres"
    address_whitelist = [
      "localhost"
    ]
    pool_size = 10

    [pools.pgml.shards.0]
    servers = [
      ["127.0.0.1", 9876, "primary"]
    ]
    database = "postgres"
    """
    cls.pgcat_process = utils.pgcat_start_with_config(config_file)
    time.sleep(1)

  @classmethod
  def teardown_class(cls):
    cls.pgcat_process.kill()
    cls.pgcat_process.wait()
    time.sleep(1)

  def test_positive(self) -> None:
    conn, cur = utils.connect_db_generic(
        username='pgcat', password='pgcat', host='127.0.0.1', database='pgcat')
    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_negative(self) -> None:
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(username='pgcat', password='pgcat', host=PRIVATE_IP_ADDRESS, database='pgcat')


class TestNormalUser:

  @classmethod
  def setup_class(cls):
    cls.pgcat_process = utils.pgcat_start_with_config(config_file)
    time.sleep(1)

  @classmethod
  def teardown_class(cls):
    cls.pgcat_process.kill()
    cls.pgcat_process.wait()
    time.sleep(1)

  def test_private_ip_range_positive(self) -> None:
    # Validate can connect with private ip
    conn, cur = utils.connect_db_generic(
        username='private_ip_range_whitelist', password='private_ip_range_whitelist', host='127.0.0.1', database="pgml")
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_private_ip_range_negative(self) -> None:
    # Validate cannot connect with public ip
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(
          username='private_ip_range_whitelist', password='private_ip_range_whitelist', host=PRIVATE_IP_ADDRESS, database="pgml")

  def test_public_ip_range_positive(self) -> None:
    # validate can conenct with private IP
    conn, cur = utils.connect_db_generic(username='public_ip_range_whitelist', password='public_ip_range_whitelist', host='127.0.0.1', database="pgml")
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_public_ip_range_negative(self) -> None:
    # validate can conenct with public IP
    conn, cur = utils.connect_db_generic(username='public_ip_range_whitelist', password='public_ip_range_whitelist', host=PRIVATE_IP_ADDRESS, database="pgml")
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_localhostname_positive(self) -> None:
    # Check able to connect with private ip
    conn, cur = utils.connect_db_generic(username='hostname_whitelist', password='hostname_whitelist', host='127.0.0.1', database="pgml")
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_localhostname_negative(self) -> None:
    # Check not able to connect with public ip
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(username='hostname_whitelist', password='hostname_whitelist', host=PRIVATE_IP_ADDRESS, database="pgml")

  def test_local_ip_positive(self) -> None:
    # Check able to connect with local ip
    conn, cur = utils.connect_db_generic(username='ip_whitelist', password='ip_whitelist', host='127.0.0.1', database="pgml")
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

  def test_local_ip_negative(self) -> None:
    # Check not able to connect with public ip
    with pytest.raises(psycopg2.OperationalError):
      conn, cur = utils.connect_db_generic(username='ip_whitelist', password='ip_whitelist', host=PRIVATE_IP_ADDRESS, database="pgml")
