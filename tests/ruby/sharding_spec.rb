# frozen_string_literal: true
require_relative 'spec_helper'


describe "Sharding" do
  let(:processes) { Helpers::Pgcat.three_shard_setup("sharded_db", 5) }

  before do
    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

    # Setup the sharding data
    3.times do |i|
      conn.exec("SET SHARD TO '#{i}'")
      conn.exec("DELETE FROM data WHERE id > 0")
    end

    18.times do |i|
      i = i + 1
      conn.exec("SET SHARDING KEY TO '#{i}'")
      conn.exec("INSERT INTO data (id, value) VALUES (#{i}, 'value_#{i}')")
    end
  end

  after do

    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  describe "automatic routing of extended protocol" do
    it "can do it" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      conn.exec("SET SERVER ROLE TO 'auto'")

      18.times do |i|
        result = conn.exec_params("SELECT * FROM data WHERE id = $1", [i + 1])
        expect(result.ntuples).to eq(1)
      end
    end

    it "can do it with multiple parameters" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      conn.exec("SET SERVER ROLE TO 'auto'")

      18.times do |i|
        result = conn.exec_params("SELECT * FROM data WHERE id = $1 AND id = $2", [i + 1, i + 1])
        expect(result.ntuples).to eq(1)
      end
    end
  end
end
