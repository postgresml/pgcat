import psycopg2
import pytest

@pytest.fixture
def conn():
	conn = psycopg2.connect("dbname=sharded_db user=sharding_user password=sharding_user port=6432 host=127.0.0.1")
	conn.autocommit = False
	return conn

def test_prepared(conn):
	cursor = conn.cursor()
	cursor.execute("SELECT id FROM data WHERE id = %s", (1,))
	result = cursor.fetchall()
	assert len(result) == 1
