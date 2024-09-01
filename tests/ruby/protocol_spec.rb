# frozen_string_literal: true
require_relative 'spec_helper'


describe "Portocol handling" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 1, "session") }
  let(:sequence) { [] }
  let(:pgcat_socket) { PostgresSocket.new('localhost', processes.pgcat.port) }
  let(:pgdb_socket) { PostgresSocket.new('localhost', processes.all_databases.first.port) }

  after do
    pgdb_socket.close
    pgcat_socket.close
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  def run_comparison(sequence, socket_a, socket_b)
    sequence.each do |msg|
      if msg.is_a?(Symbol)
        socket_a.send(msg)
        socket_b.send(msg)
      else
        socket_a.send_message(msg)
        socket_b.send_message(msg)
      end
      compare_messages(
        socket_a.read_from_server,
        socket_b.read_from_server
      )
    end
  end

  def compare_messages(msg_arr0, msg_arr1)
    if msg_arr0.count != msg_arr1.count
      error_output = []

      error_output << "#{msg_arr0.count} : #{msg_arr1.count}"
      error_output << "PgCat Messages"
      error_output += msg_arr0.map { |message| "\t#{message[:code]} - #{message[:bytes].map(&:chr).join(" ")}" }
      error_output << "PgServer Messages"
      error_output += msg_arr1.map { |message| "\t#{message[:code]} - #{message[:bytes].map(&:chr).join(" ")}" }
      error_desc = error_output.join("\n")
      raise StandardError, "Message count mismatch #{error_desc}"
    end

    (0..msg_arr0.count - 1).all? do |i|
      msg0 = msg_arr0[i]
      msg1 = msg_arr1[i]

      result = [
        msg0[:code] == msg1[:code],
        msg0[:len] == msg1[:len],
        msg0[:bytes] == msg1[:bytes],
      ].all?

      next result if result

      if result == false
        error_string = []
        if msg0[:code] != msg1[:code]
          error_string << "code #{msg0[:code]} != #{msg1[:code]}"
        end
        if msg0[:len] != msg1[:len]
          error_string << "len #{msg0[:len]} != #{msg1[:len]}"
        end
        if msg0[:bytes] != msg1[:bytes]
          error_string << "bytes #{msg0[:bytes]} != #{msg1[:bytes]}"
        end
        err = error_string.join("\n")

        raise StandardError, "Message mismatch #{err}"
      end
    end
  end

  RSpec.shared_examples "at parity with database" do
    before do
      pgcat_socket.send_startup_message("sharding_user", "sharded_db", "sharding_user")
      pgdb_socket.send_startup_message("sharding_user", "shard0", "sharding_user")
    end

    it "works" do
      run_comparison(sequence, pgcat_socket, pgdb_socket)
    end
  end

  context "Cancel Query" do
    let(:sequence) {
      [        
        SimpleQueryMessage.new("SELECT pg_sleep(5)"),
        :cancel_query
      ]
    }

    it_behaves_like "at parity with database"
  end

  xcontext "Simple query after parse" do
    let(:sequence) {
      [
        ParseMessage.new("", "SELECT 5", []),
        SimpleQueryMessage.new("SELECT 1"),
        BindMessage.new("", "", [], [], [0]),
        DescribeMessage.new("P", ""),
        ExecuteMessage.new("", 1),
        SyncMessage.new
      ]
    }

    # Known to fail due to PgCat not supporting flush
    it_behaves_like "at parity with database"
  end

  xcontext "Flush message" do
    let(:sequence) {
      [
        ParseMessage.new("", "SELECT 1", []),
        FlushMessage.new
      ]
    }

    # Known to fail due to PgCat not supporting flush
    it_behaves_like "at parity with database"
  end

  xcontext "Bind without parse" do
    let(:sequence) {
      [BindMessage.new("", "", [], [], [0])]
    }
    # This is known to fail.
    # Server responds immediately, Proxy buffers the message
    it_behaves_like "at parity with database"
  end

  context "Simple message" do
    let(:sequence) {
      [SimpleQueryMessage.new("SELECT 1")]
    }

    it_behaves_like "at parity with database"
  end
  
  10.times do |i|
    context "Extended protocol" do
      let(:sequence) {
        [
          ParseMessage.new("", "SELECT 1", []),
          BindMessage.new("", "", [], [], [0]),
          DescribeMessage.new("S", ""),
          ExecuteMessage.new("", 1),
          SyncMessage.new
        ]
      }

      it_behaves_like "at parity with database"
    end
  end

  describe "Protocol-level prepared statements" do
    let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 1, "transaction") } 
    before do 
      q_sock = PostgresSocket.new('localhost', processes.pgcat.port)
      q_sock.send_startup_message("sharding_user", "sharded_db", "sharding_user")
      table_query = "CREATE TABLE IF NOT EXISTS employees (employee_id SERIAL PRIMARY KEY, salary NUMERIC(10, 2) CHECK (salary > 0));"
      q_sock.send_message(SimpleQueryMessage.new(table_query))
      q_sock.close
      
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["sharded_db"]["prepared_statements_cache_size"] = 500
      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end
    after do 
      q_sock = PostgresSocket.new('localhost', processes.pgcat.port)
      q_sock.send_startup_message("sharding_user", "sharded_db", "sharding_user")
      table_query = "DROP TABLE IF EXISTS employees;"
      q_sock.send_message(SimpleQueryMessage.new(table_query))
      q_sock.close
    end

    context "When unnamed prepared statements are used" do
      it "does not cache them" do
        socket = PostgresSocket.new('localhost', processes.pgcat.port)
        socket.send_startup_message("sharding_user", "sharded_db", "sharding_user")
      
        socket.send_message(SimpleQueryMessage.new("DISCARD ALL"))
        socket.read_from_server
        
        10.times do |i|
          socket.send_message(ParseMessage.new("", "SELECT #{i}", []))
          socket.send_message(BindMessage.new("", "", [], [], [0]))
          socket.send_message(DescribeMessage.new("S", ""))
          socket.send_message(ExecuteMessage.new("", 1))
          socket.send_message(SyncMessage.new)
          socket.read_from_server
        end
      
        socket.send_message(SimpleQueryMessage.new("SELECT name, statement, prepare_time, parameter_types FROM pg_prepared_statements"))
        result = socket.read_from_server
        number_of_saved_statements = result.count { |m| m[:code] == 'D' }
        expect(number_of_saved_statements).to eq(0)
      end
    end

    context "When named prepared statements are used" do
      it "caches them" do
        socket = PostgresSocket.new('localhost', processes.pgcat.port)
        socket.send_startup_message("sharding_user", "sharded_db", "sharding_user")

        socket.send_message(SimpleQueryMessage.new("DISCARD ALL"))
        socket.read_from_server
        
        3.times do
          socket.send_message(ParseMessage.new("my_query", "SELECT * FROM employees WHERE employee_id in ($1,$2,$3)", [0,0,0]))
          socket.send_message(BindMessage.new("", "my_query", [0,0,0], [0,0,0].map(&:to_s), [0,0,0,0,0,0]))
          socket.send_message(SyncMessage.new)
          socket.read_from_server
        end
      
        3.times do
          socket.send_message(ParseMessage.new("my_other_query", "SELECT * FROM employees WHERE salary in ($1,$2,$3)", [0,0,0]))
          socket.send_message(BindMessage.new("", "my_other_query", [0,0,0], [0,0,0].map(&:to_s), [0,0,0,0,0,0]))
          socket.send_message(SyncMessage.new)
          socket.read_from_server
        end

        socket.send_message(SimpleQueryMessage.new("SELECT name, statement, prepare_time, parameter_types FROM pg_prepared_statements"))
        result = socket.read_from_server
        number_of_saved_statements = result.count { |m| m[:code] == 'D' }
        expect(number_of_saved_statements).to eq(2)
      end
    end

    context "When DISCARD ALL/DEALLOCATE ALL are called" do 
      it "resets server and client caches" do 
        socket = PostgresSocket.new('localhost', processes.pgcat.port)
        socket.send_startup_message("sharding_user", "sharded_db", "sharding_user")
  
        20.times do |i|
          socket.send_message(ParseMessage.new("my_query_#{i}", "SELECT * FROM employees WHERE employee_id in ($1,$2,$3)", [0,0,0]))
        end
      
        20.times do |i|
          socket.send_message(BindMessage.new("", "my_query_#{i}", [0,0,0], [0,0,0].map(&:to_s), [0,0,0,0,0,0]))
        end 
      
        socket.send_message(SyncMessage.new)
        socket.read_from_server
        
        socket.send_message(SimpleQueryMessage.new("DISCARD ALL"))
        socket.read_from_server
        responses = []
        4.times do |i|
          socket.send_message(ParseMessage.new("my_query_#{i}", "SELECT * FROM employees WHERE employee_id in ($1,$2,$3)", [0,0,0]))
          socket.send_message(BindMessage.new("", "my_query_#{i}", [0,0,0], [0,0,0].map(&:to_s), [0,0,0,0,0,0]))
          socket.send_message(SyncMessage.new)

          responses += socket.read_from_server
        end

        errors = responses.select { |message| message[:code] == 'E' }
        error_message = errors.map { |message| message[:bytes].map(&:chr).join("") }.join("\n")
        raise StandardError, "Encountered the following errors: #{error_message}" if errors.length > 0
      end
    end
    
    context "Maximum number of bound paramters" do
      it "does not crash" do
        test_socket = PostgresSocket.new('localhost', processes.pgcat.port)
        test_socket.send_startup_message("sharding_user", "sharded_db", "sharding_user")
      
        types = Array.new(65_535) { |i| 0 }

        params = Array.new(65_535) { |i| "$#{i+1}" }.join(",")
        test_socket.send_message(ParseMessage.new("my_query", "SELECT * FROM employees WHERE employee_id in (#{params})", types))

        test_socket.send_message(BindMessage.new("my_query", "my_query", types, types.map(&:to_s), types))

        test_socket.send_message(SyncMessage.new)
        
        # If the proxy crashes, this will raise an error
        expect { test_socket.read_from_server }.to_not raise_error

        test_socket.close
      end
    end
  end
end