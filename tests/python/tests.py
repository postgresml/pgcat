import psycopg2

conn = psycopg2.connect("postgres://random:password@127.0.0.1:5433/db")
cur = conn.cursor()

cur.execute("SELECT 123");
res = cur.fetchall()

print(res)