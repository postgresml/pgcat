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
    sequence.each do |msg, *args|
      socket_a.send(msg, *args)
      socket_b.send(msg, *args)

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
        [:send_query_message, "SELECT pg_sleep(5)"],
        [:cancel_query]
      ]
    }

    it_behaves_like "at parity with database"
  end

  xcontext "Simple query after parse" do
    let(:sequence) {
      [
        [:send_parse_message, "SELECT 5"],
        [:send_query_message, "SELECT 1"],
        [:send_bind_message],
        [:send_describe_message, "P"],
        [:send_execute_message],
        [:send_sync_message],
      ]
    }

    # Known to fail due to PgCat not supporting flush
    it_behaves_like "at parity with database"
  end

  xcontext "Flush message" do
    let(:sequence) {
      [
        [:send_parse_message, "SELECT 1"],
        [:send_flush_message]
      ]
    }

    # Known to fail due to PgCat not supporting flush
    it_behaves_like "at parity with database"
  end

  xcontext "Bind without parse" do
    let(:sequence) {
      [
        [:send_bind_message]
      ]
    }
    # This is known to fail.
    # Server responds immediately, Proxy buffers the message
    it_behaves_like "at parity with database"
  end

  context "Simple message" do
    let(:sequence) {
      [[:send_query_message, "SELECT 1"]]
    }

    it_behaves_like "at parity with database"
  end

  context "Extended protocol" do
    let(:sequence) {
      [
        [:send_parse_message, "SELECT 1"],
        [:send_bind_message],
        [:send_describe_message, "P"],
        [:send_execute_message],
        [:send_sync_message],
      ]
    }

    it_behaves_like "at parity with database"
  end
end
