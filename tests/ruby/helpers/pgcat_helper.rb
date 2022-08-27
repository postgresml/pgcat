require 'json'
require 'ostruct'
require_relative 'pgcat_process'

module Helpers
  module Pgcat
    def self.three_shard_setup(pool_name, pool_size)
      user = {"password" => "sharding_user", "pool_size" => pool_size, "statement_timeout" => 0, "username" => "sharding_user"}

      primary0 = PgcatProcess.new("debug")
      primary1 = PgcatProcess.new("debug")
      primary2 = PgcatProcess.new("debug")
      main     = PgcatProcess.new("debug")

      main_cfg = main.current_config

      # Traffic inspection proxies configs
      [primary0, primary1, primary2].each_with_index do |proxy, idx|
        proxy_cfg = JSON.parse(main_cfg.to_json)
        proxy_cfg["pools"] = {
          "shard#{idx}" => {
            "default_role" => "any",
            "pool_mode" => "session",
            "primary_reads_enabled" => false,
            "query_parser_enabled" => false,
            "sharding_function" => "pg_bigint_hash",
            "shards" => {
              "0" => { "database" => "shard#{idx}", "servers" => [["0.0.0.0", "5432", "primary"]] },
            },
            "users" => {"0" => user}
          },
        }
        proxy_cfg["general"]["enable_prometheus_exporter"] = false
        proxy_cfg["general"]["port"] = proxy.port
        proxy.update_config(proxy_cfg)
      end

      # Main proxy configs
      main_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => "transaction",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => { "database" => "shard0", "servers" => [["0.0.0.0", primary0.port.to_s, "primary"]] },
            "1" => { "database" => "shard1", "servers" => [["0.0.0.0", primary1.port.to_s, "primary"]] },
            "2" => { "database" => "shard2", "servers" => [["0.0.0.0", primary2.port.to_s, "primary"]] },
          },
          "users" => { "0" => user }
        }
      }
      main.update_config(main_cfg)

      [primary0, primary1, primary2, main].map(&:start)
      [primary0, primary1, primary2, main].map(&:wait_until_ready)

      OpenStruct.new.tap do |struct|
        struct.main = main
        struct.shards = [primary0, primary1, primary2]
      end
    end

    def self.single_shard_setup(pool_name, pool_size)
      user = {"password" => "sharding_user", "pool_size" => pool_size, "statement_timeout" => 0, "username" => "sharding_user"}

      primary  = PgcatProcess.new("debug")
      replica0 = PgcatProcess.new("debug")
      replica1 = PgcatProcess.new("debug")
      replica2 = PgcatProcess.new("debug")
      main     = PgcatProcess.new("debug")

      main_cfg = main.current_config

      # Traffic inspection proxies configs
      [primary, replica0, replica1, replica2].each_with_index do |proxy, idx|
        role = idx == 0 ? "primary" : "replica"
        proxy_cfg = JSON.parse(main_cfg.to_json)
        proxy_cfg["pools"] = {
          "shard0" => {
            "default_role" => "#{role}",
            "pool_mode" => "session",
            "primary_reads_enabled" => false,
            "query_parser_enabled" => false,
            "sharding_function" => "pg_bigint_hash",
            "shards" => {
              "0" => { "database" => "shard0", "servers" => [["0.0.0.0", "5432", "#{role}"]] },
            },
            "users" => {"0" => user}
          },
        }
        proxy_cfg["general"]["enable_prometheus_exporter"] = false
        proxy_cfg["general"]["port"] = proxy.port
        proxy.update_config(proxy_cfg)
      end

      # Main proxy configs
      main_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => "transaction",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [
                ["0.0.0.0", primary.port.to_s, "primary"],
                ["0.0.0.0", replica0.port.to_s, "replica"],
                ["0.0.0.0", replica1.port.to_s, "replica"],
                ["0.0.0.0", replica2.port.to_s, "replica"]
              ]
            },
          },
          "users" => { "0" => user }
        }
      }
      main_cfg["general"]["port"] = main.port
      main.update_config(main_cfg)

      [primary, replica0, replica1, replica2, main].map(&:start)
      [primary, replica0, replica1, replica2, main].map(&:wait_until_ready)

      OpenStruct.new.tap do |struct|
        struct.main = main
        struct.primary = primary
        struct.replicas = [replica0, replica1, replica2]
        struct.all = [primary, replica0, replica1, replica2]
      end
    end
  end
end
