# frozen_string_literal: true
require_relative 'spec_helper'


describe "Routing" do
  # This creates a two-layered Pgcat setup
  #        ┌─ primary  ── pg
  # main ──┼─ replica0 ── pg
  #        ├─ replica1 ── pg
  #        └─	replica2 ── pg
  # Since each intermediate pgcat represents a single instance
  # we can inspect the logs for individual intermediate processes
  # to test routing logic end-to-end whereas inspecting logs on
  # the main process or the stats can be subject to misattribution bugs
  # in the main process, for example Pgcat may report in stats that it is
  # talking to instance A whereas it is actually talking to instance B
  let(:proxies) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }
  after { proxies.all&.map(&:shutdown) }

  describe "SET ROLE" do
    context "primary" do
      it "routes queries only to primary" do
        conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'primary'")

        query_count = 30
        failed_count = 0

        proxies.all.map(&:begin_counting_queries)
        query_count.times do
          conn.async_exec("SELECT 1")
        rescue
          failed_count += 1
        end

        expect(failed_count).to eq(0)
        proxies.replicas.map(&:end_counting_queries).each do |instance_share|
          expect(instance_share).to eq(0)
        end

        # account for health checks
        expect(proxies.primary.end_counting_queries).to be_within(3).of(query_count)
      end
    end
    context "replica" do
      it "routes queries only to replicas" do
        conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'replica'")

        query_count = QUERY_COUNT
        failed_count = 0
        expected_share = query_count / proxies.replicas.count
        failed_count = 0

        proxies.all.map(&:begin_counting_queries)
        query_count.times do
          conn.async_exec("SELECT 1")
        rescue
          failed_count += 1
        end

        expect(failed_count).to eq(0)

        proxies.replicas.map(&:end_counting_queries).each do |instance_share|
          expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end

        expect(proxies.primary.end_counting_queries).to eq(0)
      end
    end

    context "any" do
      it "routes queries to all instances" do
        conn = PG.connect(proxies.main.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'any'")

        query_count = QUERY_COUNT
        failed_count = 0
        expected_share = query_count / proxies.all.count
        failed_count = 0

        proxies.all.map(&:begin_counting_queries)
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
  end
end

