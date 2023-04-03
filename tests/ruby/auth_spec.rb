# frozen_string_literal: true
require_relative 'spec_helper'


describe "Authentication" do
  describe "multiple secrets configured" do
    let(:secrets) { ["one_secret", "two_secret"] }
    let(:processes) { Helpers::Pgcat.three_shard_setup("sharded_db", 5, pool_mode="transaction", lb_mode="random", log_level="info", secrets=["one_secret", "two_secret"]) }

    after do
      processes.all_databases.map(&:reset)
      processes.pgcat.shutdown
    end

    it "can connect using all secrets and postgres password" do
      secrets.push("sharding_user").each do |secret|
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user", password=secret))
        conn.exec("SELECT current_user")
      end
    end
  end

  describe "no secrets configured" do
    let(:secrets) { [] }
    let(:processes) { Helpers::Pgcat.three_shard_setup("sharded_db", 5, pool_mode="transaction", lb_mode="random", log_level="info") }

    after do
      processes.all_databases.map(&:reset)
      processes.pgcat.shutdown
    end

    it "can connect using only the password" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      conn.exec("SELECT current_user")

      expect { PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user", password="secret_one")) }.to raise_error PG::ConnectionBad
    end
  end
end
