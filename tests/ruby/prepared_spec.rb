require_relative 'spec_helper'

describe 'Prepared statements' do
  def setup_pgcat(pool_size=5, prepared_statements_cache_size=100, server_round_robin=false)
    processes = Helpers::Pgcat.single_instance_setup('sharded_db', pool_size, "transaction")

    new_configs = processes.pgcat.current_config
    new_configs["general"]["server_round_robin"] = server_round_robin
    new_configs["pools"]["sharded_db"]["prepared_statements_cache_size"] = prepared_statements_cache_size
    processes.pgcat.update_config(new_configs)
    processes.pgcat.reload_config

    processes
  end

  context 'enabled' do
    it 'test_prepared_statement' do
      processes = setup_pgcat()
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

    it 'test_prepared_statement_params' do
      processes = setup_pgcat()
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

    it "test_parse_larger_than_pkt_buf" do
      processes = setup_pgcat()
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

    it "test_parse_large" do
      processes = setup_pgcat()
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

    it "test_bind_large" do
      processes = setup_pgcat()
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

    it "test_evict_statement_cache" do
      # cache size 1
      processes = setup_pgcat(1, 1)
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

    it "test_does_not_evict_statement_cache" do
      # cache size 5
      processes = setup_pgcat(1, 5)
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

    it "test_reuses_statement_cache_with_different_statement_name_same_connection" do
      processes = setup_pgcat(5, 5)
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

    it "test_reuses_statement_cache_with_different_statement_name_different_connection" do
      processes = setup_pgcat(5, 5)

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

    it "test_reject_name_reuse" do
      processes = setup_pgcat(1)

      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))
      conn.prepare('statement1', 'SELECT 1')
      conn.exec_prepared('statement1')

      conn.prepare('statement1', 'SELECT 1')
    end

    it "test_reload_config" do
      processes = setup_pgcat(1)

      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      # prepare query
      conn.prepare('statement1', 'SELECT 1')
      conn.exec_prepared('statement1')

      # Reload config which triggers pool recreation
      new_configs = processes.pgcat.current_config
      new_configs["pools"]["sharded_db"]["prepared_statements_cache_size"] = 5
      processes.pgcat.update_config(new_configs)
      processes.pgcat.reload_config

      # still able to run prepared query
      conn.exec_prepared('statement1')

    end
  end
end
