require 'pg'
require 'toxiproxy'

class PgInstance
  attr_reader :port
  attr_reader :username
  attr_reader :password
  attr_reader :database_name

  def initialize(port, username, password, database_name)
    @original_port = port
    @toxiproxy_port = 10000 + port.to_i
    @port = @toxiproxy_port

    @username = username
    @password = password
    @database_name = database_name
    @toxiproxy_name = "database_#{@original_port}"
    Toxiproxy.populate([{
      name: @toxiproxy_name,
      listen: "0.0.0.0:#{@toxiproxy_port}",
      upstream: "localhost:#{@original_port}",
    }])

    # Toxiproxy server will outlive our PgInstance objects
    # so we want to destroy our proxies before exiting
    # Ruby finalizer is ideal for doing this
    ObjectSpace.define_finalizer(@toxiproxy_name, proc { Toxiproxy[@toxiproxy_name].destroy })
  end

  def with_connection
    conn = PG.connect("postgres://#{@username}:#{@password}@localhost:#{port}/#{database_name}")
    yield conn
  ensure
    conn&.close
  end

  def reset
    reset_toxics
    reset_stats
    drop_connections
    sleep 0.1
  end

  def toxiproxy
    Toxiproxy[@toxiproxy_name]
  end

  def take_down
    if block_given?
      Toxiproxy[@toxiproxy_name].toxic(:limit_data, bytes: 5).apply { yield }
    else
      Toxiproxy[@toxiproxy_name].toxic(:limit_data, bytes: 5).toxics.each(&:save)
    end
  end

  def add_latency(latency)
    if block_given?
      Toxiproxy[@toxiproxy_name].toxic(:latency, latency: latency).apply { yield }
    else
      Toxiproxy[@toxiproxy_name].toxic(:latency, latency: latency).toxics.each(&:save)
    end
  end

  def delete_proxy
    Toxiproxy[@toxiproxy_name].delete
  end

  def reset_toxics
    Toxiproxy[@toxiproxy_name].toxics.each(&:destroy)
    sleep 0.1
  end

  def reset_stats
    with_connection { |c| c.async_exec("SELECT pg_stat_statements_reset()") }
  end

  def drop_connections
    username = with_connection { |c| c.async_exec("SELECT current_user")[0]["current_user"] }
    with_connection { |c| c.async_exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename='#{username}'") }
  end

  def count_connections
    with_connection { |c| c.async_exec("SELECT COUNT(*) as count FROM pg_stat_activity")[0]["count"].to_i }
  end

  def count_query(query)
    with_connection { |c| c.async_exec("SELECT SUM(calls) FROM pg_stat_statements WHERE query = '#{query}'")[0]["sum"].to_i }
  end

  def count_select_1_plus_2
    with_connection { |c| c.async_exec("SELECT SUM(calls) FROM pg_stat_statements WHERE query = 'SELECT $1 + $2'")[0]["sum"].to_i }
  end
end
