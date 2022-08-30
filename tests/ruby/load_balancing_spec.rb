# frozen_string_literal: true
require_relative 'spec_helper'

describe "Load Balancing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context "under regular circumstances" do
    it "balances query volume between all instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / processes.all_databases.count
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to eq(0)
      processes.all_databases.map(&:count_select_1_plus_2).each do |instance_share|
        expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
      end
    end
  end

  context "when some replicas are down" do
    it "balances query volume between working instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      expected_share = QUERY_COUNT / (processes.all_databases.count - 2)
      failed_count = 0

      processes[:replicas][0].take_down do
        processes[:replicas][1].take_down do
          QUERY_COUNT.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
        end
      end

      expect(failed_count).to eq(2)
      processes.all_databases.each do |instance|
        queries_routed = instance.count_select_1_plus_2
        if processes.replicas[0..1].include?(instance)
          expect(queries_routed).to eq(0)
        else
          expect(queries_routed).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end
      end
    end
  end
end

