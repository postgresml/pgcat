# frozen_string_literal: true
require 'uri'
require_relative 'spec_helper'

describe "Query Mirroing" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 10) }
  let(:pgcat_conn_str) { processes.pgcat.connection_string("sharded_db", "sharding_user") }
  let(:mirror_host) { "localhost" }

  before do
    new_configs = processes.pgcat.current_config
    new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
      [mirror_host, "5432", "0"],
      [mirror_host, "5432", "0"],
      [mirror_host, "5432", "0"],
    ]
    processes.pgcat.update_config(new_configs)
    processes.pgcat.reload_config
  end

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  it "can mirror a query" do
    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
    conn.async_exec("SELECT 1 + 2")
    expect(processes.all_databases.first.count_select_1_plus_2).to eq(4)
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
