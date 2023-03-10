# frozen_string_literal: true
require 'uri'
require_relative 'spec_helper'

describe "Query Mirroing" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 10) }
  let(:mirror_pg) { PgInstance.new(8432, "sharding_user", "sharding_user", "shard2")}
  let(:pgcat_conn_str) { processes.pgcat.connection_string("sharded_db", "sharding_user") }
  let(:mirror_host) { "localhost" }

  before do
    new_configs = processes.pgcat.current_config
    new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
      [mirror_host, mirror_pg.port.to_s, "0"],
      [mirror_host, mirror_pg.port.to_s, "0"],
      [mirror_host, mirror_pg.port.to_s, "0"],
    ]
    processes.pgcat.update_config(new_configs)
    processes.pgcat.reload_config
  end

  after do
    processes.all_databases.map(&:reset)
    mirror_pg.reset
    processes.pgcat.shutdown
  end

  it "can mirror a query" do
    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
    runs = 15
    runs.times { conn.async_exec("SELECT 1 + 2") }
    sleep 0.5
    expect(processes.all_databases.first.count_select_1_plus_2).to eq(runs)
    expect(mirror_pg.count_select_1_plus_2).to eq(runs * 3)
  end

  context "when main server connection is closed" do
    it "closes the mirror connection" do
      baseline_count = processes.all_databases.first.count_connections
      5.times do |i|
        # Force pool cycling to detect zombie mirror connections
        new_configs = processes.pgcat.current_config
        new_configs["pools"]["sharded_db"]["idle_timeout"] = 5000 + i
        new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
          [mirror_host, mirror_pg.port.to_s, "0"],
          [mirror_host, mirror_pg.port.to_s, "0"],
          [mirror_host, mirror_pg.port.to_s, "0"],
        ]
        processes.pgcat.update_config(new_configs)
        processes.pgcat.reload_config
      end
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      conn.async_exec("SELECT 1 + 2")
      sleep 0.5
      # Expect same number of connection even after pool cycling
      expect(processes.all_databases.first.count_connections).to be < baseline_count + 2
    end
  end

  xcontext "when mirror server goes down temporarily" do
    it "continues to transmit queries after recovery" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      mirror_pg.take_down do
        conn.async_exec("SELECT 1 + 2")
        sleep 0.1
      end
      10.times { conn.async_exec("SELECT 1 + 2") }
      sleep 1
      expect(mirror_pg.count_select_1_plus_2).to be >= 2
    end
  end

  context "when a mirror is down" do
    let(:mirror_host) { "badhost" }

    it "does not fail to send the main query" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      # No Errors here
      conn.async_exec("SELECT 1 + 2")
      expect(processes.all_databases.first.count_select_1_plus_2).to eq(1)
    end

    it "does not fail to send the main query (even after thousands of mirror attempts)" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      # No Errors here
      1000.times { conn.async_exec("SELECT 1 + 2") }
      expect(processes.all_databases.first.count_select_1_plus_2).to eq(1000)
    end
  end
end
