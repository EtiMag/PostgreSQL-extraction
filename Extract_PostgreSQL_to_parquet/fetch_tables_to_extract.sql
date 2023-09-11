select table_name, table_schema
from information_schema.tables
where table_schema = 'public' and table_type = 'BASE TABLE'
