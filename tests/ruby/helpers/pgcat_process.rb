require 'pg'
require 'toml'
require 'fileutils'
require 'securerandom'

class PgcatProcess
  attr_reader :port
  attr_reader :pid

  def self.finalize(pid, log_filename, config_filename)
    `kill #{pid}`
    File.delete(config_filename) if File.exist?(config_filename)
    File.delete(log_filename) if File.exist?(log_filename)
  end

  def initialize(log_level)
    @env = {"RUST_LOG" => log_level}
    @port = rand(20000..32760)
    @log_level = log_level
    @log_filename = "/tmp/pgcat_log_#{SecureRandom.urlsafe_base64}.log"
    @config_filename = "/tmp/pgcat_cfg_#{SecureRandom.urlsafe_base64}.toml"

    @command = "../../target/debug/pgcat #{@config_filename}"

    FileUtils.cp("../../pgcat.toml", @config_filename)
    cfg = current_config
    cfg["general"]["port"] = @port.to_i
    cfg["general"]["enable_prometheus_exporter"] = false

    update_config(cfg)
  end

  def logs
    File.read(@log_filename)
  end

  def update_config(config_hash)
    @original_config = current_config
    output_to_write = TOML::Generator.new(config_hash).body
    output_to_write = output_to_write.gsub(/,\s*["|'](\d+)["|']\s*,/, ',\1,')
    File.write(@config_filename, output_to_write)
  end

  def current_config
    old_cfg = File.read(@config_filename)
    loadable_string = old_cfg.gsub(/,\s*(\d+)\s*,/, ', "\1",')
    TOML.load(loadable_string)
  end

  def reload_config
    `kill -s HUP #{@pid}`
    sleep 0.1
  end

  def start
    raise StandardError, "Process is already started" unless @pid.nil?
    @pid = Process.spawn(@env, @command, err: @log_filename, out: @log_filename)
    ObjectSpace.define_finalizer(@log_filename, proc { PgcatProcess.finalize(@pid, @log_filename, @config_filename) })

    return self
  end

  def wait_until_ready
    exc = nil
    10.times do
      PG::connect(example_connection_string).close

      return self
    rescue => e
      exc = e
      sleep(0.5)
    end
    puts exc
    raise StandardError, "Process #{@pid} never became ready. Logs #{logs}"
  end

  def stop
    `kill #{@pid}`
    sleep 0.1
  end

  def shutdown
    stop
    File.delete(@config_filename) if File.exist?(@config_filename)
    File.delete(@log_filename) if File.exist?(@log_filename)
  end

  def admin_connection_string
    cfg = current_config
    username = cfg["general"]["admin_username"]
    password = cfg["general"]["admin_password"]

    "postgresql://#{username}:#{password}@0.0.0.0:#{@port}/pgcat"
  end

  def connection_string(pool_name, username)
    cfg = current_config

    user_idx, user_obj = cfg["pools"][pool_name]["users"].detect { |k, user| user["username"] == username }
    password = user_obj["password"]

    "postgresql://#{username}:#{password}@0.0.0.0:#{@port}/#{pool_name}"
  end

  def example_connection_string
    cfg = current_config
    first_pool_name = cfg["pools"].keys[0]

    db_name = first_pool_name

    username = cfg["pools"][first_pool_name]["users"]["0"]["username"]
    password = cfg["pools"][first_pool_name]["users"]["0"]["password"]

    "postgresql://#{username}:#{password}@0.0.0.0:#{@port}/#{db_name}?application_name=example_app"
  end
end
