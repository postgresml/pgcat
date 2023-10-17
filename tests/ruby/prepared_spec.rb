require_relative 'spec_helper'

describe 'Prepared statements' do
  let(:pool_size) { 5 }
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", pool_size) }
  let(:prepared_statements_cache_size) { 100 }
  let(:server_round_robin) { false }

  before do
    new_configs = processes.pgcat.current_config
    new_configs["general"]["server_round_robin"] = server_round_robin
    new_configs["pools"]["sharded_db"]["prepared_statements_cache_size"] = prepared_statements_cache_size
    new_configs["pools"]["sharded_db"]["users"]["0"]["pool_size"] =  pool_size
    processes.pgcat.update_config(new_configs)
    processes.pgcat.reload_config
  end

  context 'when trying prepared statements' do
    it 'it allows unparameterized statements to succeed' do
      conn1 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
      conn2 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      prepared_query = "SELECT 1"
      
      # prepare query on server 1 and client 1
      conn1.prepare('statement1', prepared_query)
      conn1.exec_prepared('statement1')
      
      conn2.transaction do
        # Claim server 1 with client 2
        conn2.exec("SELECT 2")
      
        # Client 1 now runs the prepared query, and it's automatically
        # prepared on server 2
        conn1.prepare('statement2', prepared_query)
        conn1.exec_prepared('statement2')
      
        # Client 2 now prepares the same query that was already
        # prepared on server 1. And PgBouncer reuses that already
        # prepared query for this different client.
        conn2.prepare('statement3', prepared_query)
        conn2.exec_prepared('statement3')
        end
    ensure
        conn1.close if conn1
        conn2.close if conn2
    end

    it 'it allows parameterized statements to succeed' do
      conn1 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
      conn2 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      prepared_query = "SELECT $1"
      
      # prepare query on server 1 and client 1
      conn1.prepare('statement1', prepared_query)
      conn1.exec_prepared('statement1', [1])
    
      conn2.transaction do
        # Claim server 1 with client 2
        conn2.exec("SELECT 2")
      
        # Client 1 now runs the prepared query, and it's automatically
        # prepared on server 2
        conn1.prepare('statement2', prepared_query)
        conn1.exec_prepared('statement2', [1])
      
        # Client 2 now prepares the same query that was already
        # prepared on server 1. And PgBouncer reuses that already
        # prepared query for this different client.
        conn2.prepare('statement3', prepared_query)
        conn2.exec_prepared('statement3', [1])
      end
    ensure
       conn1.close if conn1
       conn2.close if conn2

    end
  end

  context 'when trying large packets' do
    it "works with large parse" do
      conn1 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      long_string = "1" * 4096 * 10
      prepared_query = "SELECT '#{long_string}'"
    
  
      # prepare query on server 1 and client 1
      conn1.prepare('statement1', prepared_query)
      result = conn1.exec_prepared('statement1')
    
      # assert result matches long_string
      expect(result.getvalue(0, 0)).to eq(long_string)
    ensure
      conn1.close if conn1
    end

    it "works with large bind" do
      conn1 = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
    
      long_string = "1" * 4096 * 10
      prepared_query = "SELECT $1::text"
    
      # prepare query on server 1 and client 1
      conn1.prepare('statement1', prepared_query)
      result = conn1.exec_prepared('statement1', [long_string])
    
      # assert result matches long_string
      expect(result.getvalue(0, 0)).to eq(long_string)
    ensure
      conn1.close if conn1
    end
  end

  context 'when statement cache is smaller than set of unqiue statements' do
    let(:prepared_statements_cache_size) { 1 }
    let(:pool_size) { 1 }

    it "evicts all but 1 statement from the server cache" do
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      5.times do |i|
        prepared_query = "SELECT '#{i}'"
        conn.prepare("statement#{i}", prepared_query)
        result = conn.exec_prepared("statement#{i}")
        expect(result.getvalue(0, 0)).to eq(i.to_s)
      end

      # Check number of prepared statements (expected: 1)
      n_statements = conn.exec("SELECT count(*) FROM pg_prepared_statements").getvalue(0, 0).to_i
      expect(n_statements).to eq(1)
    end
  end

  context 'when statement cache is larger than set of unqiue statements' do
    let(:pool_size) { 1 }

    it "does not evict any of the statements from the cache" do
      # cache size 5
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      5.times do |i|
        prepared_query = "SELECT '#{i}'"
        conn.prepare("statement#{i}", prepared_query)
        result = conn.exec_prepared("statement#{i}")
        expect(result.getvalue(0, 0)).to eq(i.to_s)
      end

      # Check number of prepared statements (expected: 1)
      n_statements = conn.exec("SELECT count(*) FROM pg_prepared_statements").getvalue(0, 0).to_i
      expect(n_statements).to eq(5)
    end
  end

  context 'when preparing the same query' do
    let(:prepared_statements_cache_size) { 5 }
    let(:pool_size) { 5 }

    it "reuses statement cache when there are different statement names on the same connection" do
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      10.times do |i|
        statement_name = "statement_#{i}"
        conn.prepare(statement_name, 'SELECT $1::int')
        conn.exec_prepared(statement_name, [1])
      end

      # Check number of prepared statements (expected: 1)
      n_statements = conn.exec("SELECT count(*) FROM pg_prepared_statements").getvalue(0, 0).to_i
      expect(n_statements).to eq(1)
    end

    it "reuses statement cache when there are different statement names on different connections" do
      10.times do |i|
        conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
        statement_name = "statement_#{i}"
        conn.prepare(statement_name, 'SELECT $1::int')
        conn.exec_prepared(statement_name, [1])
      end

      # Check number of prepared statements (expected: 1)
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
      n_statements = conn.exec("SELECT count(*) FROM pg_prepared_statements").getvalue(0, 0).to_i
      expect(n_statements).to eq(1)
    end
  end

  context 'when reloading config' do
    let(:pool_size) { 1 }

    it "test_reload_config" do
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      # prepare query
      conn.prepare('statement1', 'SELECT 1')
      conn.exec_prepared('statement1')

      # Reload config which triggers pool recreation
      new_configs = processes.pgcat.current_config
      new_configs["pools"]["sharded_db"]["prepared_statements_cache_size"] = prepared_statements_cache_size + 1
      processes.pgcat.update_config(new_configs)
      processes.pgcat.reload_config

      # check that we're starting with no prepared statements on the server
      conn_check = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
      n_statements = conn_check.exec("SELECT count(*) FROM pg_prepared_statements").getvalue(0, 0).to_i
      expect(n_statements).to eq(0)

      # still able to run prepared query
      conn.exec_prepared('statement1')
    end
  end
end
