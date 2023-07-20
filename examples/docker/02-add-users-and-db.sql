CREATE ROLE myappadmin PASSWORD 'myappadmin' LOGIN;
CREATE DATABASE myapp owner myappadmin;
GRANT ALL PRIVILEGES ON DATABASE myapp TO myappadmin;
