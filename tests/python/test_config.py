
import pytest
import subprocess
import tempfile
import time
import utils
from jinja2 import Environment, BaseLoader

template_config = """
{% if invalid_config_type == 'global' %}
[non_existant_section]
non_existant_parameter = "arbitrary_value"
{% endif %}

{% if invalid_config_type == 'plugins' %}
[plugins.nonexistent]
enabled = true
{% endif %}

[plugins.prewarmer]
enabled = false
queries = [
  "SELECT pg_prewarm('pgbench_accounts')",
]
{% if invalid_config_type == 'plugins_prewarmer' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[plugins.query_logger]
enabled = false
{% if invalid_config_type == 'plugins_query_logger' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[plugins.table_access]
enabled = false
tables = [
  "pg_user",
  "pg_roles",
  "pg_database",
]
{% if invalid_config_type == 'plugins_table_access' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[plugins.intercept]
enabled = true
{% if invalid_config_type == 'plugins_intercept' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[plugins.intercept.queries.0]

query = "select current_database() as a, current_schemas(false) as b"
schema = [
  ["a", "text"],
  ["b", "text"],
]
result = [
  ["${DATABASE}", "{public}"],
]
{% if invalid_config_type == 'plugins_intercept_queries' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[general]
host = "0.0.0.0"
port = 6433
admin_username = "pgcat"
admin_password = "pgcat"
{% if invalid_config_type == 'general' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[pools.pgml]
{% if invalid_config_type == 'pools' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[pools.pgml.users.0]
username = "simple_user"
password = "simple_user"
pool_size = 10
min_pool_size = 1
pool_mode = "transaction"
{% if invalid_config_type == 'user' %}
non_existant_parameter = "arbitrary_value"
{% endif %}

[pools.pgml.shards.0]
servers = [
  ["127.0.0.1", 5432, "primary"]
]
database = "some_db"
{% if invalid_config_type == 'pool' %}
non_existant_parameter = "arbitrary_value"
{% endif %}
"""

parameters = [
    'global', 'general', 'user', 'pool', 'plugins', 'plugins_prewarmer', 'plugins_query_logger', 'plugins_table_access', 'plugins_intercept', 'plugins_intercept_queries', 'pools']
@pytest.mark.parametrize("invalid_config_type", parameters)
def test_negative(invalid_config_type: str):
    rtemplate = Environment(loader=BaseLoader).from_string(template_config)
    data = rtemplate.render(invalid_config_type=invalid_config_type)

    print(data)
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        f.write(data)

    process = subprocess.Popen(["./target/debug/pgcat", tmp.name], shell=False)
    time.sleep(3)
    poll = process.poll()
    try:
        assert poll is not None
    except AssertionError as e:
        process.kill()
        process.wait()
        raise
