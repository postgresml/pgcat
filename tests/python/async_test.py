import psycopg2
import asyncio
import asyncpg


def regular_main():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="sharded_db",
        user="sharding_user",
        password="sharding_user",
        port=6432,
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a SQL query
    cur.execute("SELECT 1")

    # Fetch the results
    rows = cur.fetchall()

    # Print the results
    for row in rows:
        print(row[0])

    # Close the cursor and the database connection
    cur.close()
    conn.close()


async def main():
    # Connect to the PostgreSQL database
    conn = await asyncpg.connect(
        host="localhost",
        database="sharded_db",
        user="sharding_user",
        password="sharding_user",
        port=6432,
    )

    # Execute a SQL query
    for _ in range(25):
        rows = await conn.fetch("SELECT 1")

        # Print the results
        for row in rows:
            print(row[0])

    # Close the database connection
    await conn.close()


regular_main()
asyncio.run(main())
