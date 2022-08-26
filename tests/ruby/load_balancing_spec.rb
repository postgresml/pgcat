# frozen_string_literal: true
require_relative 'spec_helper'

describe "Load Balancing" do
  let(:proxies) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }

  context "under regular circumstances" do
    it "balances query volume between all instances" do
      conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
      proxies.all.map(&:begin_counting_queries)

      query_count = 300
      expected_share = query_count / 4.0
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1")
      rescue
        failed_count += 1
      end

      expect(failed_count).to eq(0)
      proxies.all.map(&:end_counting_queries).each do |instance_share|
        expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
      end
    end
  end

  context "when some replicas are down" do
    before do
      proxies[:replicas][0].stop
      proxies[:replicas][1].stop
    end

    it "balances query volume between working instances" do
      conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
      proxies.all.map(&:begin_counting_queries)

      query_count = 300
      expected_share = query_count / 2.0
      failed_count = 0
      query_count.times do
        conn.async_exec("SELECT 1")
      rescue
        conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
        failed_count += 1
      end

      expect(failed_count).to eq(2)
      proxies.all.each do |instance|
        queries_routed = instance.end_counting_queries
        if proxies.replicas[0..1].include?(instance)
          expect(queries_routed).to eq(0)
        else
          expect(queries_routed).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end
      end
    end
  end
end

