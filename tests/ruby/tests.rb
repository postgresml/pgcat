require 'pg'

conn = PG.connect(host: '127.0.0.1', port: 5433, dbname: 'test')

conn.exec( "SELECT * FROM pg_stat_activity" ) do |result|
  puts "     PID | User             | Query"
  result.each do |row|
    puts " %7d | %-16s | %s " %
      row.values_at('pid', 'usename', 'query')
  end
end