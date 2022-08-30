# frozen_string_literal: true
require_relative 'spec_helper'


describe "Routing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  describe "SET ROLE" do
    context "primary" do
      it "routes queries only to primary" do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'primary'")

        query_count = 30
        failed_count = 0

        query_count.times do
          conn.async_exec("SELECT 1 + 2")
        rescue
          failed_count += 1
        end

        expect(failed_count).to eq(0)
        processes.replicas.map(&:count_select_1_plus_2).each do |instance_share|
          expect(instance_share).to eq(0)
        end

        expect(processes.primary.count_select_1_plus_2).to eq(query_count)
      end
    end
    context "replica" do
      it "routes queries only to replicas" do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'replica'")

        expected_share = QUERY_COUNT / processes.replicas.count
        failed_count = 0

        QUERY_COUNT.times do
          conn.async_exec("SELECT 1 + 2")
        rescue
          failed_count += 1
        end

        expect(failed_count).to eq(0)

        processes.replicas.map(&:count_select_1_plus_2).each do |instance_share|
          expect(instance_share).to be_within(expected_share * MARGIN_OF_ERROR).of(expected_share)
        end

        expect(processes.primary.count_select_1_plus_2).to eq(0)
      end
    end

    context "any" do
      it "routes queries to all instances" do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        conn.async_exec("SET SERVER ROLE to 'any'")

        expected_share = QUERY_COUNT / processes.all_databases.count
        failed_count = 0

        QUERY_COUNT.times do
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
  end
end
