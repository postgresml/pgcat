import psycopg2

def test_normal_db_access():
    conn = psycopg2.connect("postgres://random:password@127.0.0.1:6432/db")
    cur = conn.cursor()

    cur.execute("SELECT 1")
    res = cur.fetchall()

    print(res)


def test_admin_db_access():
    conn = psycopg2.connect("postgres://user:pass@127.0.0.1:6432/pgcat")
    conn.autocommit = True # BEGIN/COMMIT is not supported by admin db
    cur = conn.cursor()

    cur.execute("SHOW POOLS")
    res = cur.fetchall()
    print(res)



test_normal_db_access()
test_admin_db_access()
