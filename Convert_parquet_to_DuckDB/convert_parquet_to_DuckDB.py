import os
import duckdb
import yaml
import logging
logging.basicConfig(level=logging.DEBUG)

"""Create or update duckdb database using the input parquet files"""

with open("Convert_parquet_to_DuckDB/paths.yaml", "r") as paths_file:
    paths: dict = yaml.safe_load(paths_file)

data_dir: str = paths["input_directory_parquet_files"]
duckdb_database_path: str = paths["duckdb_database_path"]

connection: duckdb.DuckDBPyConnection = duckdb.connect(database=duckdb_database_path)

# import all parquet files from data_dir
for dir_name in os.listdir(data_dir):
    path_dir_table: str = os.path.join(data_dir, dir_name)
    if not os.path.isdir(path_dir_table):
        continue
    schema_name, table_name = dir_name.split(".")

    try:
        # first create view to check if parquet files can be read, don't modify the database if they can't
        connection.sql(f"CREATE VIEW view_parquet_files AS SELECT * FROM '{path_dir_table}/*.parquet'")

        connection.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        connection.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
        connection.sql(f"CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM view_parquet_files")
        connection.sql("DROP VIEW view_parquet_files")

    except duckdb.InvalidInputException as e:
        logging.warning(f"Could not create or update table {schema_name}.{table_name} because of exception: \n {e} \n"
                        f"This probably means that the parquet files are empty")
