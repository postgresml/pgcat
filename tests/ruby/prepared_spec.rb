require_relative 'spec_helper'

describe 'Prepared statements' do
  let(:processes) { Helpers::Pgcat.three_shard_setup('sharded_db', 5) }

  context 'enabled' do
    it 'will work over the same connection' do
      conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

      10.times do |i|
        statement_name = "statement_#{i}"
        conn.prepare(statement_name, 'SELECT $1::int')
        conn.exec_prepared(statement_name, [1])
        conn.describe_prepared(statement_name)
      end
    end

    it 'will work with new connections' do
      10.times do
        conn = PG.connect(processes.pgcat.connection_string('sharded_db', 'sharding_user'))

        statement_name = 'statement1'
        conn.prepare('statement1', 'SELECT $1::int')
        conn.exec_prepared('statement1', [1])
        conn.describe_prepared('statement1')
      end
    end
  end
end
