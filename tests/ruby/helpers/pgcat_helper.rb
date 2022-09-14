require 'json'
require 'ostruct'
require_relative 'pgcat_process'
require_relative 'pg_instance'

module Helpers
  module Pgcat
    def self.three_shard_setup(pool_name, pool_size, pool_mode="transaction")
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat    = PgcatProcess.new("info")
      primary0 = PgInstance.new(5432, user["username"], user["password"], "shard0")
      primary1 = PgInstance.new(7432, user["username"], user["password"], "shard1")
      primary2 = PgInstance.new(8432, user["username"], user["password"], "shard2")

      pgcat_cfg = pgcat.current_config
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => pool_mode,
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => { "database" => "shard0", "servers" => [["localhost", primary0.port.to_s, "primary"]] },
            "1" => { "database" => "shard1", "servers" => [["localhost", primary1.port.to_s, "primary"]] },
            "2" => { "database" => "shard2", "servers" => [["localhost", primary2.port.to_s, "primary"]] },
          },
          "users" => { "0" => user }
        }
      }
      pgcat.update_config(pgcat_cfg)

      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.shards = [primary0, primary1, primary2]
        struct.all_databases = [primary0, primary1, primary2]
      end
    end

    def self.single_instance_setup(pool_name, pool_size, pool_mode="transaction")
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat = PgcatProcess.new("trace")
      pgcat_cfg = pgcat.current_config

      primary  = PgInstance.new(5432, user["username"], user["password"], "shard0")

      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [
                ["localhost", primary.port.to_s, "primary"]
              ]
            },
          },
          "users" => { "0" => user }
        }
      }
      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.all_databases = [primary]
      end
    end

    def self.single_shard_setup(pool_name, pool_size, pool_mode="transaction")
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat = PgcatProcess.new("info")
      pgcat_cfg = pgcat.current_config

      primary  = PgInstance.new(5432, user["username"], user["password"], "shard0")
      replica0 = PgInstance.new(7432, user["username"], user["password"], "shard0")
      replica1 = PgInstance.new(8432, user["username"], user["password"], "shard0")
      replica2 = PgInstance.new(9432, user["username"], user["password"], "shard0")

      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => pool_mode,
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [
                ["localhost", primary.port.to_s, "primary"],
                ["localhost", replica0.port.to_s, "replica"],
                ["localhost", replica1.port.to_s, "replica"],
                ["localhost", replica2.port.to_s, "replica"]
              ]
            },
          },
          "users" => { "0" => user }
        }
      }
      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.replicas = [replica0, replica1, replica2]
        struct.all_databases = [primary, replica0, replica1, replica2]
      end
    end
  end
end
