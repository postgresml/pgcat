require "active_record"

ActiveRecord.verbose_query_logs = true
ActiveRecord::Base.logger = Logger.new(STDOUT)

ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  host: "127.0.0.1",
  port: 6432,
  username: "sharding_user",
  password: "sharding_user",
  database: "rails_dev",
  prepared_statements: false, # Transaction mode
  advisory_locks: false, # Same
)

class TestTable < ActiveRecord::Base
  self.table_name = "test_table"
end

class TestSafeTable < ActiveRecord::Base
  self.table_name = "test_safe_table"
end

class ShouldNeverHappenException < Exception
end

# # Create the table.
class CreateTestTable < ActiveRecord::Migration[7.0]
  # Disable transasctions or things will fly out of order!
  disable_ddl_transaction!

  SHARDS = 3

  def change
    SHARDS.times do |x|
      # This will make this migration reversible!
      reversible do
        connection.execute "SET SHARD TO '#{x.to_i}'"
        connection.execute "SET SERVER ROLE TO 'primary'"
      end

      # Always wrap the entire migration inside a transaction. If that's not possible,
      # execute a `SET SHARD` command before every statement and make sure AR doesn't need
      # to load database information beforehand (i.e. it's not the first query in the migration).
      connection.transaction do
        create_table :test_table, if_not_exists: true do |t|
          t.string :name
          t.string :description

          t.timestamps
        end
      end
    end
  end
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
      connection.execute "DROP TABLE test_safe_table CASCADE"
    end
  end
end

20.times do
  begin
    CreateTestTable.migrate(:down)
  rescue Exception
    puts "Tables don't exist yet"
  end

  begin
    CreateSafeShardedTable.migrate(:down)
  rescue Exception
    puts "Tables don't exist yet"
  end

  CreateTestTable.migrate(:up)
  CreateSafeShardedTable.migrate(:up)

  3.times do |x|
    TestSafeTable.connection.execute "SET SHARD TO '#{x.to_i}'"
    TestSafeTable.connection.execute "SET SERVER ROLE TO 'primary'"
    TestSafeTable.connection.execute "TRUNCATE #{TestTable.table_name}"
  end

  10.times do |x|
    x += 1 # Postgres ids start at 1
    TestSafeTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
    TestSafeTable.connection.execute "SET SERVER ROLE TO 'primary'"
    TestSafeTable.create(id: x, name: "something_special_#{x.to_i}", description: "It's a surprise!")
  end

  10.times do |x|
    x += 1 # 0 confuses our sharding function
    TestSafeTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
    TestSafeTable.connection.execute "SET SERVER ROLE TO 'replica'"
    TestSafeTable.find_by_id(x).id
  end
end

# Test wrong shard
TestSafeTable.connection.execute "SET SHARD TO '1'"
begin
  TestSafeTable.create(id: 5, name: "test", description: "test description")
  raise ShouldNeverHappenException("Uh oh")
rescue ActiveRecord::StatementInvalid
  puts "OK"
end
