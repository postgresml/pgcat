import psycopg2

conn = psycopg2.connect("postgres://random:password@127.0.0.1:6432/db")
cur = conn.cursor()

cur.execute("SELECT 1");
res = cur.fetchall()

print(res)

# conn.commit()