# PgCat Configurations
## `general` Section

### host
```
path: general.host
default: "0.0.0.0"
```

What IP to run on, 0.0.0.0 means accessible from everywhere.

### port
```
path: general.port
default: 6432
```

Port to run on, same as PgBouncer used in this example.

### enable_prometheus_exporter
```
path: general.enable_prometheus_exporter
default: true
```

Whether to enable prometheus exporter or not.

### prometheus_exporter_port
```
path: general.prometheus_exporter_port
default: 9930
```

Port at which prometheus exporter listens on.

### connect_timeout
```
path: general.connect_timeout
default: 1000 # milliseconds
```

How long the client waits to obtain a server connection before aborting (ms).
This is similar to PgBouncer's `query_wait_timeout`.

### idle_timeout
```
path: general.idle_timeout
default: 30000 # milliseconds
```

How long an idle connection with a server is left open (ms).

### server_lifetime
```
path: general.server_lifetime
default: 86400000 # 24 hours
```

Max connection lifetime before it's closed, even if actively used.

### server_round_robin
```
path: general.server_round_robin
default: false
```

Whether to use round robin for server selection or not.

### server_tls
```
path: general.server_tls
default: false
```

Whether to use TLS for server connections or not.

### verify_server_certificate
```
path: general.verify_server_certificate
default: false
```

Whether to verify server certificate or not.

### verify_config
```
path: general.verify_config
default: true
```

Whether to verify config or not.

### idle_client_in_transaction_timeout
```
path: general.idle_client_in_transaction_timeout
default: 0 # milliseconds
```

How long a client is allowed to be idle while in a transaction (ms).

### healthcheck_timeout
```
path: general.healthcheck_timeout
default: 1000 # milliseconds
```

How much time to give the health check query to return with a result (ms).

### healthcheck_delay
```
path: general.healthcheck_delay
default: 30000 # milliseconds
```

How long to keep connection available for immediate re-use, without running a healthcheck query on it

### shutdown_timeout
```
path: general.shutdown_timeout
default: 60000 # milliseconds
```

How much time to give clients during shutdown before forcibly killing client connections (ms).

### ban_time
```
path: general.ban_time
default: 60 # seconds
```

How long to ban a server if it fails a health check (seconds).

### log_client_connections
```
path: general.log_client_connections
default: false
```

If we should log client connections

### log_client_disconnections
```
path: general.log_client_disconnections
default: false
```

If we should log client disconnections

### autoreload
```
path: general.autoreload
default: 15000 # milliseconds
```

When set, PgCat automatically reloads its configurations at the specified interval (in milliseconds) if it detects changes in the configuration file. The default interval is 15000 milliseconds or 15 seconds.

### worker_threads
```
path: general.worker_threads
default: 5
```

Number of worker threads the Runtime will use (4 by default).

### tcp_keepalives_idle
```
path: general.tcp_keepalives_idle
default: 5
```

Number of seconds of connection idleness to wait before sending a keepalive packet to the server.

### tcp_keepalives_count
```
path: general.tcp_keepalives_count
default: 5
```

Number of unacknowledged keepalive packets allowed before giving up and closing the connection.

### tcp_keepalives_interval
```
path: general.tcp_keepalives_interval
default: 5
```

### tcp_user_timeout
```
path: general.tcp_user_timeout
default: 10000
```
A linux-only parameters that defines the amount of time in milliseconds that transmitted data may remain unacknowledged or buffered data may remain untransmitted (due to zero window size) before TCP will forcibly disconnect


### tls_certificate
```
path: general.tls_certificate
default: <UNSET>
example: "server.cert"
```

Path to TLS Certificate file to use for TLS connections

### tls_private_key
```
path: general.tls_private_key
default: <UNSET>
example: "server.key"
```

Path to TLS private key file to use for TLS connections

### admin_username
```
path: general.admin_username
default: "admin_user"
```

User name to access the virtual administrative database (pgbouncer or pgcat)
Connecting to that database allows running commands like `SHOW POOLS`, `SHOW DATABASES`, etc..

### admin_password
```
path: general.admin_password
default: "admin_pass"
```

Password to access the virtual administrative database

### auth_query
```
path: general.auth_query
default: <UNSET>
example: "SELECT $1"
```

Query to be sent to servers to obtain the hash used for md5 authentication. The connection will be
established using the database configured in the pool. This parameter is inherited by every pool
and can be redefined in pool configuration.

### auth_query_user
```
path: general.auth_query_user
default: <UNSET>
example: "sharding_user"
```

User to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### auth_query_password
```
path: general.auth_query_password
default: <UNSET>
example: "sharding_user"
```

Password to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### dns_cache_enabled
```
path: general.dns_cache_enabled
default: false
```
When enabled, ip resolutions for server connections specified using hostnames will be cached
and checked for changes every `dns_max_ttl` seconds. If a change in the host resolution is found
old ip connections are closed (gracefully) and new connections will start using new ip.

### dns_max_ttl
```
path: general.dns_max_ttl
default: 30
```
Specifies how often (in seconds) cached ip addresses for servers are rechecked (see `dns_cache_enabled`).

## `pools.<pool_name>` Section

### pool_mode
```
path: pools.<pool_name>.pool_mode
default: "transaction"
```

Pool mode (see PgBouncer docs for more).
`session` one server connection per connected client
`transaction` one server connection per client transaction

### load_balancing_mode
```
path: pools.<pool_name>.load_balancing_mode
default: "random"
```

Load balancing mode
`random` selects the server at random
`loc` selects the server with the least outstanding busy connections

### default_role
```
path: pools.<pool_name>.default_role
default: "any"
```

If the client doesn't specify, PgCat routes traffic to this role by default.
`any` round-robin between primary and replicas,
`replica` round-robin between replicas only without touching the primary,
`primary` all queries go to the primary unless otherwise specified.

### replica_to_primary_failover_enabled
```
path: pools.<pool_name>.replica_to_primary_failover_enabled
default: "false"
```

If set to true, when the specified role is `replica` (either by setting `default_role` or manually)
and all replicas are banned, queries will be sent to the primary (until a replica is back online).

### prepared_statements_cache_size
```
path: general.prepared_statements_cache_size
default: 0
```

Size of the prepared statements cache. 0 means disabled.
TODO: update documentation

### query_parser_enabled
```
path: pools.<pool_name>.query_parser_enabled
default: true
```

If Query Parser is enabled, we'll attempt to parse
every incoming query to determine if it's a read or a write.
If it's a read query, we'll direct it to a replica. Otherwise, if it's a write,
we'll direct it to the primary.

### primary_reads_enabled
```
path: pools.<pool_name>.primary_reads_enabled
default: true
```

If the query parser is enabled and this setting is enabled, the primary will be part of the pool of databases used for
load balancing of read queries. Otherwise, the primary will only be used for write
queries. The primary can always be explicitly selected with our custom protocol.

### sharding_key_regex
```
path: pools.<pool_name>.sharding_key_regex
default: <UNSET>
example: '/\* sharding_key: (\d+) \*/'
```

Allow sharding commands to be passed as statement comments instead of
separate commands. If these are unset this functionality is disabled.

### sharding_function
```
path: pools.<pool_name>.sharding_function
default: "pg_bigint_hash"
```

So what if you wanted to implement a different hashing function,
or you've already built one and you want this pooler to use it?
Current options:
`pg_bigint_hash`: PARTITION BY HASH (Postgres hashing function)
`sha1`: A hashing function based on SHA1

### auth_query
```
path: pools.<pool_name>.auth_query
default: <UNSET>
example: "SELECT $1"
```

Query to be sent to servers to obtain the hash used for md5 authentication. The connection will be
established using the database configured in the pool. This parameter is inherited by every pool
and can be redefined in pool configuration.

### auth_query_user
```
path: pools.<pool_name>.auth_query_user
default: <UNSET>
example: "sharding_user"
```

User to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### auth_query_password
```
path: pools.<pool_name>.auth_query_password
default: <UNSET>
example: "sharding_user"
```

Password to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### automatic_sharding_key
```
path: pools.<pool_name>.automatic_sharding_key
default: <UNSET>
example: "data.id"
```

Automatically parse this from queries and route queries to the right shard!

### idle_timeout
```
path: pools.<pool_name>.idle_timeout
default: 40000
```

Idle timeout can be overwritten in the pool

### connect_timeout
```
path: pools.<pool_name>.connect_timeout
default: 3000
```

Connect timeout can be overwritten in the pool

## `pools.<pool_name>.users.<user_index>` Section

### username
```
path: pools.<pool_name>.users.<user_index>.username
default: "sharding_user"
```

PostgreSQL username used to authenticate the user and connect to the server
if `server_username` is not set.

### password
```
path: pools.<pool_name>.users.<user_index>.password
default: "sharding_user"
```

PostgreSQL password used to authenticate the user and connect to the server
if `server_password` is not set.

### server_username
```
path: pools.<pool_name>.users.<user_index>.server_username
default: <UNSET>
example: "another_user"
```

PostgreSQL username used to connect to the server.

### server_password
```
path: pools.<pool_name>.users.<user_index>.server_password
default: <UNSET>
example: "another_password"
```

PostgreSQL password used to connect to the server.

### pool_size
```
path: pools.<pool_name>.users.<user_index>.pool_size
default: 9
```

Maximum number of server connections that can be established for this user.
The maximum number of connection from a single Pgcat process to any database in the cluster
is the sum of pool_size across all users.

### min_pool_size
```
path: pools.<pool_name>.users.<user_index>.min_pool_size
default: 0
```

Minimum number of idle server connections to retain for this pool.

### statement_timeout
```
path: pools.<pool_name>.users.<user_index>.statement_timeout
default: 0
```

Maximum query duration. Dangerous, but protects against DBs that died in a non-obvious way.
0 means it is disabled.

### connect_timeout
```
path: pools.<pool_name>.users.<user_index>.connect_timeout
default: <UNSET> # milliseconds
```

How long the client waits to obtain a server connection before aborting (ms).
This is similar to PgBouncer's `query_wait_timeout`.
If unset, uses the `connect_timeout` defined globally.

## `pools.<pool_name>.shards.<shard_index>` Section

### servers
```
path: pools.<pool_name>.shards.<shard_index>.servers
default: [["127.0.0.1", 5432, "primary"], ["localhost", 5432, "replica"]]
```

Array of servers in the shard, each server entry is an array of `[host, port, role]`

### mirrors
```
path: pools.<pool_name>.shards.<shard_index>.mirrors
default: <UNSET>
example: [["1.2.3.4", 5432, 0], ["1.2.3.4", 5432, 1]]
```

Array of mirrors for the shard, each mirror entry is an array of `[host, port, index of server in servers array]`
Traffic hitting the server identified by the index will be sent to the mirror.

### database
```
path: pools.<pool_name>.shards.<shard_index>.database
default: "shard0"
```

Database name (e.g. "postgres")
