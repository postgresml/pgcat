# frozen_string_literal: true
require 'uri'
require_relative 'spec_helper'

describe "Admin" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 10) }
  let(:pgcat_conn_str) { processes.pgcat.connection_string("sharded_db", "sharding_user") }

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  describe "SHOW STATS" do
    context "clients connect and make one query" do
      it "updates *_query_time and *_wait_time" do
        connection = PG::connect("#{pgcat_conn_str}?application_name=one_query")
        connection.async_exec("SELECT pg_sleep(0.25)")
        connection.async_exec("SELECT pg_sleep(0.25)")
        connection.async_exec("SELECT pg_sleep(0.25)")
        connection.close

        # wait for averages to be calculated, we shouldn't do this too often
        sleep(15.5)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW STATS")[0]
        admin_conn.close
        expect(results["total_query_time"].to_i).to be_within(200).of(750)
        expect(results["avg_query_time"].to_i).to_not eq(0)

        expect(results["total_wait_time"].to_i).to_not eq(0)
        expect(results["avg_wait_time"].to_i).to_not eq(0)
      end
    end
  end

  describe "SHOW POOLS" do
    context "bad credentials" do
      it "does not change any stats" do
        bad_password_url = URI(pgcat_conn_str)
        bad_password_url.password = "wrong"
        expect { PG::connect("#{bad_password_url.to_s}?application_name=bad_password") }.to raise_error(PG::ConnectionBad)

        sleep(1)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end

        expect(results["sv_idle"]).to eq("1")
      end
    end

    context "bad database name" do
      it "does not change any stats" do
        bad_db_url = URI(pgcat_conn_str)
        bad_db_url.path = "/wrong_db"
        expect { PG::connect("#{bad_db_url.to_s}?application_name=bad_db") }.to raise_error(PG::ConnectionBad)

        sleep(1)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end

        expect(results["sv_idle"]).to eq("1")
      end
    end

    context "client connects but issues no queries" do
      it "only affects cl_idle stats" do
        connections = Array.new(20) { PG::connect(pgcat_conn_str) }
        sleep(1)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["cl_idle"]).to eq("20")
        expect(results["sv_idle"]).to eq("1")

        connections.map(&:close)
        sleep(1.1)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_active cl_idle cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["sv_idle"]).to eq("1")
      end
    end

    context "clients connect and make one query" do
      it "only affects cl_idle, sv_idle stats" do
        connections = Array.new(5) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          Thread.new { c.async_exec("SELECT pg_sleep(2.5)") }
        end

        sleep(1.1)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_waiting cl_cancel_req sv_idle sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["cl_active"]).to eq("5")
        expect(results["sv_active"]).to eq("5")

        sleep(3)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["cl_idle"]).to eq("5")
        expect(results["sv_idle"]).to eq("5")

        connections.map(&:close)
        sleep(1)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["sv_idle"]).to eq("5")
      end
    end

    context "client connects and opens a transaction and closes connection uncleanly" do
      it "produces correct statistics" do
        connections = Array.new(5) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          Thread.new do
            c.async_exec("BEGIN")
            c.async_exec("SELECT pg_sleep(0.01)")
            c.close
          end
        end

        sleep(1.1)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["sv_idle"]).to eq("5")
      end
    end

    context "client fail to checkout connection from the pool" do
      it "counts clients as idle" do
        new_configs = processes.pgcat.current_config
        new_configs["general"]["connect_timeout"] = 500
        new_configs["general"]["ban_time"] = 1
        new_configs["general"]["shutdown_timeout"] = 1
        new_configs["pools"]["sharded_db"]["users"]["0"]["pool_size"] = 1
        processes.pgcat.update_config(new_configs)
        processes.pgcat.reload_config

        threads = []
        connections = Array.new(5) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          threads << Thread.new { c.async_exec("SELECT pg_sleep(1)") rescue PG::SystemError }
        end

        sleep(2)
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["cl_idle"]).to eq("5")
        expect(results["sv_idle"]).to eq("1")

        threads.map(&:join)
        connections.map(&:close)
      end
    end

    context "clients connects and disconnect normally" do
      let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 2) }

      it 'shows the same number of clients before and after' do
        clients_before = clients_connected_to_pool(processes: processes)
        threads = []
        connections = Array.new(4) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          threads << Thread.new { c.async_exec("SELECT 1") }
        end
        clients_between = clients_connected_to_pool(processes: processes)
        expect(clients_before).not_to eq(clients_between)
        connections.each(&:close)
        clients_after = clients_connected_to_pool(processes: processes)
        expect(clients_before).to eq(clients_after)
      end
    end

    context "clients connects and disconnect abruptly" do
      let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 10) }

      it 'shows the same number of clients before and after' do
        threads = []
        connections = Array.new(2) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          threads << Thread.new { c.async_exec("SELECT 1") }
        end
        clients_before = clients_connected_to_pool(processes: processes)
        random_string = (0...8).map { (65 + rand(26)).chr }.join
        connection_string = "#{pgcat_conn_str}?application_name=#{random_string}"
        faulty_client = Process.spawn("psql -Atx #{connection_string} >/dev/null")
        sleep(1)
        # psql starts two processes, we only know the pid of the parent, this
        # ensure both are killed
        `pkill -9 -f '#{random_string}'`
        Process.wait(faulty_client)
        clients_after = clients_connected_to_pool(processes: processes)
        expect(clients_before).to eq(clients_after)
      end
    end

    context "clients overwhelm server pools" do
      let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 2) }

      it "cl_waiting is updated to show it" do
        threads = []
        connections = Array.new(4) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          threads << Thread.new { c.async_exec("SELECT pg_sleep(1.5)") }
        end

        sleep(1.1) # Allow time for stats to update
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_idle cl_cancel_req sv_idle sv_used sv_tested sv_login maxwait].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end

        expect(results["cl_waiting"]).to eq("2")
        expect(results["cl_active"]).to eq("2")
        expect(results["sv_active"]).to eq("2")

        sleep(2.5) # Allow time for stats to update
        results = admin_conn.async_exec("SHOW POOLS")[0]
        %w[cl_active cl_waiting cl_cancel_req sv_active sv_used sv_tested sv_login].each do |s|
          raise StandardError, "Field #{s} was expected to be 0 but found to be #{results[s]}" if results[s] != "0"
        end
        expect(results["cl_idle"]).to eq("4")
        expect(results["sv_idle"]).to eq("2")

        threads.map(&:join)
        connections.map(&:close)
      end

      it "show correct max_wait" do
        threads = []
        connections = Array.new(4) { PG::connect("#{pgcat_conn_str}?application_name=one_query") }
        connections.each do |c|
          threads << Thread.new { c.async_exec("SELECT pg_sleep(1.5)") }
        end

        sleep(2.5) # Allow time for stats to update
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)
        results = admin_conn.async_exec("SHOW POOLS")[0]

        expect(results["maxwait"]).to eq("1")
        expect(results["maxwait_us"].to_i).to be_within(200_000).of(500_000)

        sleep(4.5) # Allow time for stats to update
        results = admin_conn.async_exec("SHOW POOLS")[0]
        expect(results["maxwait"]).to eq("0")

        threads.map(&:join)
        connections.map(&:close)
      end
    end
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
        c.async_exec("SELECT 4")
        c.async_exec("SELECT 5")
        c.async_exec("COMMIT")
      end

      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      sleep(1) # Wait for stats to be updated

      results = admin_conn.async_exec("SHOW CLIENTS")
      expect(results.count).to eq(3)
      normal_client_results = results.reject { |r| r["database"] == "pgcat" }
      expect(normal_client_results[0]["transaction_count"]).to eq("4")
      expect(normal_client_results[1]["transaction_count"]).to eq("4")
      expect(normal_client_results[0]["query_count"]).to eq("7")
      expect(normal_client_results[1]["query_count"]).to eq("7")

      admin_conn.close
      connections.map(&:close)
    end
  end

  describe "Manual Banning" do
    let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 10) }
    before do
      new_configs = processes.pgcat.current_config
      # Prevent immediate unbanning when we ban localhost
      new_configs["pools"]["sharded_db"]["shards"]["0"]["servers"][0][0] = "127.0.0.1"
      new_configs["pools"]["sharded_db"]["shards"]["0"]["servers"][1][0] = "127.0.0.1"
      processes.pgcat.update_config(new_configs)
      processes.pgcat.reload_config
    end

    describe "BAN/UNBAN and SHOW BANS" do
      it "bans/unbans hosts" do
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)

        # Returns a list of the banned addresses
        results = admin_conn.async_exec("BAN localhost 10").to_a
        expect(results.count).to eq(2)
        expect(results.map{ |r| r["host"] }.uniq).to eq(["localhost"])

        # Subsequent calls should yield no results
        results = admin_conn.async_exec("BAN localhost 10").to_a
        expect(results.count).to eq(0)

        results = admin_conn.async_exec("SHOW BANS").to_a
        expect(results.count).to eq(2)
        expect(results.map{ |r| r["host"] }.uniq).to eq(["localhost"])

        # Returns a list of the unbanned addresses
        results = admin_conn.async_exec("UNBAN localhost").to_a
        expect(results.count).to eq(2)
        expect(results.map{ |r| r["host"] }.uniq).to eq(["localhost"])

        # Subsequent calls should yield no results
        results = admin_conn.async_exec("UNBAN localhost").to_a
        expect(results.count).to eq(0)

        results = admin_conn.async_exec("SHOW BANS").to_a
        expect(results.count).to eq(0)
      end

      it "honors ban duration" do
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)

        # Returns a list of the banned addresses
        results = admin_conn.async_exec("BAN localhost 1").to_a
        expect(results.count).to eq(2)
        expect(results.map{ |r| r["host"] }.uniq).to eq(["localhost"])

        sleep(2)

        # After 2 seconds the ban should be lifted
        results = admin_conn.async_exec("SHOW BANS").to_a
        expect(results.count).to eq(0)
      end

      it "can handle bad input" do
        admin_conn = PG::connect(processes.pgcat.admin_connection_string)

        expect { admin_conn.async_exec("BAN").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("BAN a").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("BAN a a").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("BAN a -5").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("BAN a 0").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("BAN a a a").to_a }.to raise_error(PG::SystemError)
        expect { admin_conn.async_exec("UNBAN").to_a }.to raise_error(PG::SystemError)
      end
    end
  end

  describe "SHOW users" do
    it "returns the right users" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      results = admin_conn.async_exec("SHOW USERS")[0]
      admin_conn.close
      expect(results["name"]).to eq("sharding_user")
      expect(results["pool_mode"]).to eq("transaction")
    end
  end
end
