# frozen_string_literal: true
require_relative 'spec_helper'


describe "Miscellaneous" do
  let(:proxies) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }

  describe "Clients closing connection in the middle of transaction" do
    it "sends a rollback to the server" do
      conn = PG::connect(proxies.main.connection_string("sharded_db", "sharding_user"))
      conn.async_exec("SET SERVER ROLE to 'primary'")
      conn.async_exec("BEGIN")
      proxies.primary.begin_counting_queries
      conn.close
      sleep 0.5
      # ROLLBACK, DISCARD ALL, SET application_name
      expect(proxies.primary.end_counting_queries).to eq(3)
    end
  end

  describe "Server version reporting" do
    it "reports correct version for normal and admin databases" do
      server_conn = PG::connect(proxies.main.connection_string("sharded_db", "sharding_user"))
      expect(server_conn.server_version).not_to eq(0)
      server_conn.close


      admin_conn = PG::connect(proxies.main.admin_connection_string)
      expect(admin_conn.server_version).not_to eq(0)
      admin_conn.close
    end
  end
end

