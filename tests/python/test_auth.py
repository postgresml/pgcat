import utils
import signal

class TestTrustAuth:
    @classmethod
    def setup_method(cls):
        config= """
        [general]
        host = "0.0.0.0"
        port = 6432
        admin_username = "admin_user"
        admin_password = ""
        admin_auth_type = "trust"

        [pools.sharded_db.users.0]
        username = "sharding_user"
        password = "sharding_user"
        auth_type = "trust"
        pool_size = 10
        min_pool_size = 1
        pool_mode = "transaction"

        [pools.sharded_db.shards.0]
        servers = [
          [ "127.0.0.1", 5432, "primary" ],
        ]
        database = "shard0"
        """
        utils.pgcat_generic_start(config)

    @classmethod
    def teardown_method(self):
        utils.pg_cat_send_signal(signal.SIGTERM)

    def test_admin_trust_auth(self):
        conn, cur = utils.connect_db_trust(admin=True)
        cur.execute("SHOW POOLS")
        res = cur.fetchall()
        print(res)
        utils.cleanup_conn(conn, cur)

    def test_normal_trust_auth(self):
        conn, cur = utils.connect_db_trust(autocommit=False)
        cur.execute("SELECT 1")
        res = cur.fetchall()
        print(res)
        utils.cleanup_conn(conn, cur)

class TestMD5Auth:
    @classmethod
    def setup_method(cls):
        utils.pgcat_start()

    @classmethod
    def teardown_method(self):
        utils.pg_cat_send_signal(signal.SIGTERM)

    def test_normal_db_access(self):
        conn, cur = utils.connect_db(autocommit=False)
        cur.execute("SELECT 1")
        res = cur.fetchall()
        print(res)
        utils.cleanup_conn(conn, cur)

    def test_admin_db_access(self):
        conn, cur = utils.connect_db(admin=True)

        cur.execute("SHOW POOLS")
        res = cur.fetchall()
        print(res)
        utils.cleanup_conn(conn, cur)
