# frozen_string_literal: true
require_relative 'spec_helper'


describe "Sharding" do
  let(:processes) { Helpers::Pgcat.three_shard_setup("sharded_db", 5) }

  before do
    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
    # Setup the sharding data
    3.times do |i|
      conn.exec("SET SHARD TO '#{i}'")

      conn.exec("DELETE FROM data WHERE id > 0") rescue nil
    end

    18.times do |i|
      i = i + 1
      conn.exec("SET SHARDING KEY TO '#{i}'")
      conn.exec("INSERT INTO data (id, value) VALUES (#{i}, 'value_#{i}')")
    end

    conn.close
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

  describe "comment-based routing" do
    context "when no configs are set" do
      it "routes queries with a shard_id comment to the default shard" do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
        10.times { conn.async_exec("/* shard_id: 2 */ SELECT 1 + 2") }

        expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([10, 0, 0])
      end

      it "does not honor no_shard_specified_behavior directives" do
      end
    end

    [
      ["shard_id_regex", "/\\* the_shard_id: (\\d+) \\*/", "/* the_shard_id: 1 */"],
      ["sharding_key_regex", "/\\* the_sharding_key: (\\d+) \\*/", "/* the_sharding_key: 3 */"],
    ].each do |config_name, config_value, comment_to_use|
      context "when #{config_name} config is set" do
        let(:no_shard_specified_behavior) { nil }

        before do
          admin_conn = PG::connect(processes.pgcat.admin_connection_string)

          current_configs = processes.pgcat.current_config
          current_configs["pools"]["sharded_db"][config_name] = config_value
          if no_shard_specified_behavior
            current_configs["pools"]["sharded_db"]["no_shard_specified_behavior"] = no_shard_specified_behavior
          end
          processes.pgcat.update_config(current_configs)
          processes.pgcat.reload_config
        end

        it "routes queries with a shard_id comment to the correct shard" do
          conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
          25.times { conn.async_exec("#{comment_to_use} SELECT 1 + 2") }

          expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([0, 25, 0])
        end

        context "when no_shard_specified_behavior config is set to random" do
          let(:no_shard_specified_behavior) { "random" }

          context "with no shard comment" do
            it "sends queries to random shard" do
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              25.times { conn.async_exec("SELECT 1 + 2") }

              expect(processes.all_databases.map(&:count_select_1_plus_2).all?(&:positive?)).to be true
            end
          end

          context "with a shard comment" do
            it "honors the comment" do
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              25.times { conn.async_exec("#{comment_to_use} SELECT 1 + 2") }

              expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([0, 25, 0])
            end
          end
        end

        context "when no_shard_specified_behavior config is set to random_healthy" do
          let(:no_shard_specified_behavior) { "random_healthy" }

          context "with no shard comment" do
            it "sends queries to random healthy shard" do

              good_databases = [processes.all_databases[0], processes.all_databases[2]]
              bad_database = processes.all_databases[1]

              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              250.times { conn.async_exec("SELECT 99") }
              bad_database.take_down do
                250.times do
                  conn.async_exec("SELECT 99")
                rescue PG::ConnectionBad => e
                  conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
                end
              end

              # Routes traffic away from bad shard
              25.times { conn.async_exec("SELECT 1 + 2") }
              expect(good_databases.map(&:count_select_1_plus_2).all?(&:positive?)).to be true
              expect(bad_database.count_select_1_plus_2).to eq(0)

              # Routes traffic to the bad shard if the shard_id is specified
              25.times { conn.async_exec("#{comment_to_use} SELECT 1 + 2") }
              bad_database = processes.all_databases[1]
              expect(bad_database.count_select_1_plus_2).to eq(25)
            end
          end

          context "with a shard comment" do
            it "honors the comment" do
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              25.times { conn.async_exec("#{comment_to_use} SELECT 1 + 2") }

              expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([0, 25, 0])
            end
          end
        end

        context "when no_shard_specified_behavior config is set to shard_x" do
          let(:no_shard_specified_behavior) { "shard_2" }

          context "with no shard comment" do
            it "sends queries to the specified shard" do
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              25.times { conn.async_exec("SELECT 1 + 2") }

              expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([0, 0, 25])
            end
          end

          context "with a shard comment" do
            it "honors the comment" do
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              25.times { conn.async_exec("#{comment_to_use} SELECT 1 + 2") }

              expect(processes.all_databases.map(&:count_select_1_plus_2)).to eq([0, 25, 0])
            end
          end
        end
      end
    end
  end
end
