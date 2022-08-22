# frozen_string_literal: true

require 'active_record'
require 'pg'
require 'toml'

$stdout.sync = true

# Uncomment these two to see all queries.
# ActiveRecord.verbose_query_logs = true
# ActiveRecord::Base.logger = Logger.new(STDOUT)

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  host: '127.0.0.1',
  port: 6432,
  username: 'sharding_user',
  password: 'sharding_user',
  database: 'sharded_db',
  application_name: 'testing_pgcat',
  prepared_statements: false, # Transaction mode
  advisory_locks: false # Same
)

class TestSafeTable < ActiveRecord::Base
  self.table_name = 'test_safe_table'
end

class ShouldNeverHappenException < RuntimeError
end

class CreateSafeShardedTable < ActiveRecord::Migration[7.0]
  # Disable transasctions or things will fly out of order!
  disable_ddl_transaction!

  SHARDS = 3

  def up
    SHARDS.times do |x|
      # This will make this migration reversible!
      connection.execute "SET SHARD TO '#{x.to_i}'"
      connection.execute "SET SERVER ROLE TO 'primary'"

      connection.execute <<-SQL
        CREATE TABLE test_safe_table (
          id BIGINT PRIMARY KEY,
          name VARCHAR,
          description TEXT
        ) PARTITION BY HASH (id);

        CREATE TABLE test_safe_table_data PARTITION OF test_safe_table
        FOR VALUES WITH (MODULUS #{SHARDS.to_i}, REMAINDER #{x.to_i});
      SQL
    end
  end

  def down
    SHARDS.times do |x|
      connection.execute "SET SHARD TO '#{x.to_i}'"
      connection.execute "SET SERVER ROLE TO 'primary'"
      connection.execute 'DROP TABLE test_safe_table CASCADE'
    end
  end
end

SHARDS = 3

2.times do
  begin
    CreateSafeShardedTable.migrate(:down)
  rescue Exception
    puts "Tables don't exist yet"
  end

  CreateSafeShardedTable.migrate(:up)

  SHARDS.times do |x|
    TestSafeTable.connection.execute "SET SHARD TO '#{x.to_i}'"
    TestSafeTable.connection.execute "SET SERVER ROLE TO 'primary'"
    TestSafeTable.connection.execute "TRUNCATE #{TestSafeTable.table_name}"
  end

  # Equivalent to Makara's stick_to_master! except it sticks until it's changed.
  TestSafeTable.connection.execute "SET SERVER ROLE TO 'primary'"

  200.times do |x|
    x += 1 # Postgres ids start at 1
    TestSafeTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
    TestSafeTable.create(id: x, name: "something_special_#{x.to_i}", description: "It's a surprise!")
  end

  TestSafeTable.connection.execute "SET SERVER ROLE TO 'replica'"

  100.times do |x|
    x += 1 # 0 confuses our sharding function
    TestSafeTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
    TestSafeTable.find_by_id(x).id
  end

  # Will use the query parser to direct reads to replicas
  TestSafeTable.connection.execute "SET SERVER ROLE TO 'auto'"

  100.times do |x|
    x += 101
    TestSafeTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
    TestSafeTable.find_by_id(x).id
  end
end

# Test wrong shard
TestSafeTable.connection.execute "SET SHARD TO '1'"
begin
  TestSafeTable.create(id: 5, name: 'test', description: 'test description')
  raise ShouldNeverHappenException('Uh oh')
rescue ActiveRecord::StatementInvalid
  puts 'OK'
end

# Test evil clients
def poorly_behaved_client
  conn = PG::connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db?application_name=testing_pgcat")
  conn.async_exec 'BEGIN'
  conn.async_exec 'SELECT 1'

  conn.close
  puts 'Bad client ok'
end

25.times do
  poorly_behaved_client
end


def test_server_parameters
  server_conn = PG::connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db?application_name=testing_pgcat")
  raise StandardError, "Bad server version" if server_conn.server_version == 0
  server_conn.close

  admin_conn = PG::connect("postgres://admin_user:admin_pass@127.0.0.1:6432/pgcat")
  raise StandardError, "Bad server version" if admin_conn.server_version == 0
  admin_conn.close

  puts 'Server parameters ok'
end


class ConfigEditor
  def initialize
    @original_config_text = File.read('../../.circleci/pgcat.toml')
    text_to_load = @original_config_text.gsub("5432", "\"5432\"")

    @original_configs = TOML.load(text_to_load)
  end

  def original_configs
    TOML.load(TOML::Generator.new(@original_configs).body)
  end

  def with_modified_configs(new_configs)
    text_to_write = TOML::Generator.new(new_configs).body
    text_to_write = text_to_write.gsub("\"5432\"", "5432")
    File.write('../../.circleci/pgcat.toml', text_to_write)
    yield
  ensure
    File.write('../../.circleci/pgcat.toml', @original_config_text)
  end

end


def test_reload_pool_recycling
  admin_conn = PG::connect("postgres://admin_user:admin_pass@127.0.0.1:6432/pgcat")
  server_conn = PG::connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db?application_name=testing_pgcat")

  server_conn.async_exec("BEGIN")
  conf_editor = ConfigEditor.new
  new_configs = conf_editor.original_configs

  # swap shards
  new_configs["pools"]["sharded_db"]["shards"]["0"]["database"] = "shard1"
  new_configs["pools"]["sharded_db"]["shards"]["1"]["database"] = "shard0"

  raise StandardError if server_conn.async_exec("SELECT current_database();")[0]["current_database"] != 'shard0'
  conf_editor.with_modified_configs(new_configs) { admin_conn.async_exec("RELOAD") }
  raise StandardError if server_conn.async_exec("SELECT current_database();")[0]["current_database"] != 'shard0'
  server_conn.async_exec("COMMIT;")

  # Transaction finished, client should get new configs
  raise StandardError if server_conn.async_exec("SELECT current_database();")[0]["current_database"] != 'shard1'
  server_conn.close()

  # New connection should get new configs
  server_conn = PG::connect("postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db?application_name=testing_pgcat")
  raise StandardError if server_conn.async_exec("SELECT current_database();")[0]["current_database"] != 'shard1'

ensure
  admin_conn.async_exec("RELOAD") # Go back to old state
  admin_conn.close
  server_conn.close
  puts "Pool Recycling okay!"
  puts "Pool Recycling okay!"
end

test_reload_pool_recycling



def with_captured_stdout_stderr
  sout = STDOUT.clone
  serr = STDERR.clone
  STDOUT.reopen("/tmp/out.txt", "w+")
  STDERR.reopen("/tmp/err.txt", "w+")
  yield
  return File.read('/tmp/out.txt'), File.read('/tmp/err.txt')
ensure
  STDOUT.reopen(sout)
  STDERR.reopen(serr)
end

def test_extended_protocol_pooler_errors
  admin_conn = PG::connect("postgres://admin_user:admin_pass@127.0.0.1:6432/pgcat")

  conf_editor = ConfigEditor.new
  new_configs = conf_editor.original_configs

  # shorter timeouts
  new_configs["connect_timeout"] = 100
  conf_editor.with_modified_configs(new_configs) { admin_conn.async_exec("RELOAD") }

  conn_str = "postgres://sharding_user:sharding_user@127.0.0.1:6432/sharded_db"
  conn_under_test = PG::connect(conn_str)
  50.times do
    Thread.new do
      conn = PG::connect(conn_str)
      conn.async_exec("SELECT pg_sleep(8)") rescue PG::SystemError
    ensure
      conn&.close
    end
  end

  sleep 1
  stdout, stderr = with_captured_stdout_stderr do
    2.times do |i|
      conn_under_test.exec_params("SELECT #{i} + $1", [i]) rescue PG::SystemError
      sleep 2
    end
  end

  raise StandardError if stderr.include?("arrived from server while idle")
ensure
  admin_conn.async_exec("RELOAD") # Reset state
  conn_under_test&.close
end

test_extended_protocol_pooler_errors
