require 'pg'
def clog_pool(time)
  Thread.new do
    conn = PG::connect("postgres://sharding_user:sharding_user@localhost:6432/sharded_db")
    conn.exec_params("SELECT pg_sleep(#{time})")
  end
end

clog_pool(10)
conn = PG::connect("postgres://sharding_user:sharding_user@localhost:6432/sharded_db")
12.times do
  conn.exec_params("SELECT $1", [1]) rescue PG::SystemError
  sleep(1)
end

