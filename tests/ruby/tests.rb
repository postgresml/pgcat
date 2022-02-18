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

begin
  CreateTestTable.migrate(:down)
rescue Exception
  puts "Tables don't exist yet"
end

CreateTestTable.migrate(:up)

10.times do |x|
  x += 1 # Postgres ids start at 1
  r = TestTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"

  # Always wrap writes inside explicit transactions like these because ActiveRecord may fetch table info
  # before actually issuing the `INSERT` statement. This ensures that that happens inside a transaction
  # and the write goes to the correct shard.
  TestTable.connection.transaction do
    TestTable.create(id: x, name: "something_special_#{x.to_i}", description: "It's a surprise!")
  end
end

10.times do |x|
  x += 1 # 0 confuses our sharding function
  TestTable.connection.execute "SET SHARDING KEY TO '#{x.to_i}'"
  puts TestTable.find_by_id(x).id
end
