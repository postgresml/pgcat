module Helpers
  module AuthQuery
    def self.single_shard_auth_query(
          pg_user:,
          config_user:,
          pool_name:,
          extra_conf: {},
          log_level: 'debug',
          wait_until_ready: true
        )

      user = {
        "pool_size" => 10,
        "statement_timeout" => 0,
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config.deep_merge(extra_conf)

      primary  = PgInstance.new(5432,  pg_user["username"], pg_user["password"], "shard0")
      replica  = PgInstance.new(10432, pg_user["username"], pg_user["password"], "shard0")

      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => "transaction",
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [
                ["localhost", primary.port.to_s, "primary"],
                ["localhost", replica.port.to_s, "replica"],
              ]
            },
          },
          "users" => { "0" => user.merge(config_user) }
        }
      }
      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      
      pgcat.wait_until_ready(
        pgcat.connection_string(
          "sharded_db",
          pg_user['username'],
          pg_user['password']
        )
      ) if wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.replicas = [replica]
        struct.all_databases = [primary]
      end
    end

    def self.two_pools_auth_query(
          pg_user:,
          config_user:,
          pool_names:,
          extra_conf: {},
          log_level: 'debug'
        )

      user = {
        "pool_size" => 10,
        "statement_timeout" => 0,
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config

      primary  = PgInstance.new(5432,  pg_user["username"], pg_user["password"], "shard0")
      replica  = PgInstance.new(10432, pg_user["username"], pg_user["password"], "shard0")

      pool_template = Proc.new do |database|
        {
          "default_role" => "any",
          "pool_mode" => "transaction",
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => database,
              "servers" => [
                ["localhost", primary.port.to_s, "primary"],
                ["localhost", replica.port.to_s, "replica"],
              ]
            },
          },
          "users" => { "0" => user.merge(config_user) }
        }                                  
      end
      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_names[0]}" => pool_template.call("shard0"),
        "#{pool_names[1]}" => pool_template.call("shard1")
      }

      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg.deep_merge(extra_conf))
      pgcat.start
      
      pgcat.wait_until_ready(pgcat.connection_string("sharded_db0", pg_user['username'], pg_user['password']))

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.replicas = [replica]
        struct.all_databases = [primary]
      end
    end

    def self.create_query_auth_function(user)
      return <<-SQL
CREATE OR REPLACE FUNCTION public.user_lookup(in i_username text, out uname text, out phash text)
RETURNS record AS $$
BEGIN
    SELECT usename, passwd FROM pg_catalog.pg_shadow
    WHERE usename = i_username INTO uname, phash;
    RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION public.user_lookup(text) TO #{user};
SQL
    end

    def self.exec_in_instances(query:, instance_ports: [ 5432, 10432 ], database: 'postgres', user: 'postgres', password: 'postgres')
      instance_ports.each do |port|
        c = PG.connect("postgres://#{user}:#{password}@localhost:#{port}/#{database}")
        c.exec(query)
        c.close
      end
    end

    def self.set_up_auth_query_for_user(user:, password:, instance_ports: [ 5432, 10432 ], database: 'shard0' )
      instance_ports.each do |port|
        connection = PG.connect("postgres://postgres:postgres@localhost:#{port}/#{database}")
        connection.exec(self.drop_query_auth_function(user)) rescue PG::UndefinedFunction
        connection.exec("DROP ROLE #{user}") rescue PG::UndefinedObject
        connection.exec("CREATE ROLE #{user} ENCRYPTED PASSWORD '#{password}' LOGIN;")
        connection.exec(self.create_query_auth_function(user))
        connection.close
      end
    end

    def self.tear_down_auth_query_for_user(user:, password:, instance_ports: [ 5432, 10432 ], database: 'shard0' )
      instance_ports.each do |port|
        connection = PG.connect("postgres://postgres:postgres@localhost:#{port}/#{database}")
        connection.exec(self.drop_query_auth_function(user)) rescue PG::UndefinedFunction
        connection.exec("DROP ROLE #{user}")
        connection.close
      end
    end

    def self.drop_query_auth_function(user)
      return <<-SQL
REVOKE ALL ON FUNCTION public.user_lookup(text) FROM public, #{user};
DROP FUNCTION public.user_lookup(in i_username text, out uname text, out phash text);
SQL
    end
  end
end
