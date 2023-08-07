CREATE ROLE connection_pooler PASSWORD 'user-look-up-pass' LOGIN;
CREATE SCHEMA IF NOT EXISTS pgcat;

CREATE OR REPLACE FUNCTION pgcat.user_lookup(i_username text)
    RETURNS table ("user" text, hash text) AS $$
SELECT usename as user, passwd as hash FROM pg_catalog.pg_shadow
WHERE usename = i_username;
$$ LANGUAGE sql SECURITY DEFINER;

-- usage:
-- SELECT * FROM pgcat.user_lookup('$1');

GRANT CONNECT ON DATABASE postgres TO  connection_pooler;
GRANT USAGE ON SCHEMA pgcat TO connection_pooler;

REVOKE ALL ON FUNCTION pgcat.user_lookup(text) FROM public, connection_pooler;
GRANT EXECUTE ON FUNCTION pgcat.user_lookup(text) TO connection_pooler;
