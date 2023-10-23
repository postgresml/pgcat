
-- \setrandom aid 1 :naccounts
\set aid random(1, 100000)
-- \setrandom bid 1 :nbranches
\set bid random(1, 100000)
-- \setrandom tid 1 :ntellers
\set tid random(1, 100000)
-- \setrandom delta -5000 5000
\set delta random(-5000,5000)

\set shard random(0, 2)

BEGIN;

UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;

SELECT abalance FROM pgbench_accounts WHERE aid = :aid;

UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;

UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;

INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);

SELECT * FROM pgbench_accounts limit 1;

END;
