ALTER SYSTEM SET password_encryption to 'md5';
SELECT pg_reload_conf();
