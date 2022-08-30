# frozen_string_literal: true

require 'pg'
require_relative 'helpers/pgcat_helper'

QUERY_COUNT = 300
MARGIN_OF_ERROR = 0.30

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
