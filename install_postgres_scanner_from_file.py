import duckdb


# extension file postgres_scanner.duckdb_extension must be in wdir
duckdb.execute("INSTALL 'postgres_scanner.duckdb_extension'")
