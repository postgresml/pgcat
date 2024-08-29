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

  describe "SHOW USERS" do
    it "returns the right users" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      results = admin_conn.async_exec("SHOW USERS")[0]
      admin_conn.close
      expect(results["name"]).to eq("sharding_user")
      expect(results["pool_mode"]).to eq("transaction")
    end
  end

  describe "SHOW " do
    it "does not panic" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      expect { admin_conn.async_exec("SHOW ") }.to raise_error(PG::SystemError).with_message(/FATAL:  Unsupported SHOW query against the admin database/)
      admin_conn.close
    end
  end

  describe "SHOW <WRONG COMMAND>" do
    it "does not panic" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      ["ME THE MONEY", "ME THE WAY", "UP", "TIME"].each do |cmd|
        expect { admin_conn.async_exec("SHOW #{cmd}") }.to raise_error(PG::SystemError).with_message(/FATAL:  Unsupported SHOW query against the admin database/)
      end
      admin_conn.close
    end
  end

  describe "PAUSE" do
    it "pauses all pools" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      results = admin_conn.async_exec("SHOW DATABASES").to_a
      expect(results.map{ |r| r["paused"] }.uniq).to eq(["0"])

      admin_conn.async_exec("PAUSE")

      results = admin_conn.async_exec("SHOW DATABASES").to_a
      expect(results.map{ |r| r["paused"] }.uniq).to eq(["1"])

      admin_conn.async_exec("RESUME")

      results = admin_conn.async_exec("SHOW DATABASES").to_a
      expect(results.map{ |r| r["paused"] }.uniq).to eq(["0"])
    end

    it "handles errors" do
      admin_conn = PG::connect(processes.pgcat.admin_connection_string)
      expect { admin_conn.async_exec("PAUSE foo").to_a }.to raise_error(PG::SystemError)
      expect { admin_conn.async_exec("PAUSE foo,bar").to_a }.to raise_error(PG::SystemError)
    end
  end
end
