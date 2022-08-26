require 'json'
require 'ostruct'
require_relative 'pgcat_process'

module Helpers
  module Pgcat
    def self.single_shard_setup(pool_name, pool_size)
      primary  = PgcatProcess.new("debug")
      replica0 = PgcatProcess.new("debug")
      replica1 = PgcatProcess.new("debug")
      replica2 = PgcatProcess.new("debug")

      main = PgcatProcess.new("debug")
      cfg = main.current_config
      user = {"password" => "sharding_user", "pool_size" => pool_size, "statement_timeout" => 0, "username" => "sharding_user"}
      cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => "transaction",
          "primary_reads_enabled" => true,
          "query_parser_enabled" => true,
          "sharding_function" => "pg_bigint_hash",
          "shards" => { "0" => { "database" => "shard0", "servers" => [] } },
          "users" => { "0" => user }
        }
      }

      primary_cfg = JSON.parse(cfg.to_json)
      replica0_cfg = JSON.parse(cfg.to_json)
      replica1_cfg = JSON.parse(cfg.to_json)
      replica2_cfg = JSON.parse(cfg.to_json)
      main_cfg = JSON.parse(cfg.to_json)

      primary_cfg["pools"][pool_name]["shards"]["0"]["servers"]  = [["0.0.0.0", "5432", "primary"]]
      primary_cfg["general"]["port"] = primary.port
      primary_cfg["pools"]["shard0"] = primary_cfg["pools"][pool_name]
      primary_cfg["pools"].delete(pool_name)

      replica0_cfg["pools"][pool_name]["shards"]["0"]["servers"] = [["0.0.0.0", "5432", "replica"]]
      replica0_cfg["general"]["port"] = replica0.port
      replica0_cfg["pools"]["shard0"] = replica0_cfg["pools"][pool_name]
      replica0_cfg["pools"].delete(pool_name)

      replica1_cfg["pools"][pool_name]["shards"]["0"]["servers"] = [["0.0.0.0", "5432", "replica"]]
      replica1_cfg["general"]["port"] = replica1.port
      replica1_cfg["pools"]["shard0"] = replica1_cfg["pools"][pool_name]
      replica1_cfg["pools"].delete(pool_name)

      replica2_cfg["pools"][pool_name]["shards"]["0"]["servers"] = [["0.0.0.0", "5432", "replica"]]
      replica2_cfg["general"]["port"] = replica2.port
      replica2_cfg["pools"]["shard0"] = replica2_cfg["pools"][pool_name]
      replica2_cfg["pools"].delete(pool_name)

      main_cfg["pools"][pool_name]["shards"]["0"]["servers"] = [
        ["0.0.0.0", primary.port.to_s, "primary"],
        ["0.0.0.0", replica0.port.to_s, "replica"],
        ["0.0.0.0", replica1.port.to_s, "replica"],
        ["0.0.0.0", replica2.port.to_s, "replica"]
      ]
      main_cfg["general"]["port"] = main.port

      primary.update_config(primary_cfg)
      replica0.update_config(replica0_cfg)
      replica1.update_config(replica1_cfg)
      replica2.update_config(replica2_cfg)
      main.update_config(main_cfg)

      primary.start
      replica0.start
      replica1.start
      replica2.start
      main.start

      primary.wait_until_ready
      replica0.wait_until_ready
      replica1.wait_until_ready
      replica2.wait_until_ready
      main.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.main = main
        struct.primary = primary
        struct.replicas = [replica0, replica1, replica2]
        struct.all = [primary, replica0, replica1, replica2]
      end
    end
  end
end
