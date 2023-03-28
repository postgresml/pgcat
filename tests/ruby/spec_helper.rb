# frozen_string_literal: true

require 'pg'
require_relative 'helpers/pgcat_helper'

QUERY_COUNT = 300
MARGIN_OF_ERROR = 0.35

def with_captured_stdout_stderr
  sout = STDOUT.clone
  serr = STDERR.clone
  STDOUT.reopen("/tmp/out.txt", "w+")
  STDERR.reopen("/tmp/err.txt", "w+")
  STDOUT.sync = true
  STDERR.sync = true
  yield
  return File.read('/tmp/out.txt'), File.read('/tmp/err.txt')
ensure
  STDOUT.reopen(sout)
  STDERR.reopen(serr)
end

def clients_connected_to_pool(pool_index: 0, processes:)
  admin_conn = PG::connect(processes.pgcat.admin_connection_string)
  results = admin_conn.async_exec("SHOW POOLS")[pool_index]
  admin_conn.close
  results['cl_idle'].to_i + results['cl_active'].to_i + results['cl_waiting'].to_i
end
