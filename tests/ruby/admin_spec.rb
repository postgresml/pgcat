# frozen_string_literal: true
require_relative 'spec_helper'

describe "Admin" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  describe "SHOW CLIENTS" do
    it "reports correct number and application names" do
      conn_str = processes.pgcat.connection_string("sharded_db", "sharding_user")
      connections = Array.new(20) { |i| PG::connect("#{conn_str}?application_name=app#{i % 5}") }

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      sleep(1) # Wait for stats to be updated

      results = admin_conn.async_exec("SHOW CLIENTS")
      expect(results.count).to eq(21) # count admin clients
      expect(results.select { |c| c["application_name"] == "app3" ||  c["application_name"] == "app4" }.count).to eq(8)
      expect(results.select { |c| c["database"] == "pgcat" }.count).to eq(1)

      connections[0..5].map(&:close)
      sleep(1) # Wait for stats to be updated
      results = admin_conn.async_exec("SHOW CLIENTS")
      expect(results.count).to eq(15)

      connections[6..].map(&:close)
      sleep(1) # Wait for stats to be updated
      expect(admin_conn.async_exec("SHOW CLIENTS").count).to eq(1)
      admin_conn.close
    end

    it "reports correct number of queries and transactions" do
      conn_str = processes.pgcat.connection_string("sharded_db", "sharding_user")

      connections = Array.new(2) { |i| PG::connect("#{conn_str}?application_name=app#{i}") }
      connections.each do |c|
        c.async_exec("SELECT 1")
        c.async_exec("SELECT 2")
        c.async_exec("SELECT 3")
        c.async_exec("BEGIN")
        c.async_exec("COMMIT")
      end

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      sleep(1) # Wait for stats to be updated

      results = admin_conn.async_exec("SHOW CLIENTS")
      expect(results.count).to eq(3)
      normal_client_results = results.reject { |r| r["database"] == "pgcat" }
      expect(normal_client_results[0]["transaction_count"]).to eq("4")
      expect(normal_client_results[1]["transaction_count"]).to eq("4") # Obviously wrong, will fix in a follow up PR
      expect(normal_client_results[0]["query_count"]).to eq("5")
      expect(normal_client_results[1]["query_count"]).to eq("5")

      # puts processes.pgcat.logs

      admin_conn.close
      connections.map(&:close)
    end
  end

end
