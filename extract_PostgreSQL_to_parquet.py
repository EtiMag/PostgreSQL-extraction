import os
import getpass
import logging
import yaml
import sqlalchemy
import pandas as pd
import duckdb
import time

logging.basicConfig(level=logging.DEBUG)

duckdb.load_extension("postgres")

with open("parameters.yml", "r") as parameters_file:
    parameters: dict = yaml.safe_load(parameters_file)

output_dir: str = parameters["output_directory"]
extraction_parameters: dict = parameters["extraction_parameters"]
types_to_cast_to_string: set[str] = set(extraction_parameters["types_PostgreSQL_to_cast_to_string"])
types_postgres_to_exclude: set[str] = set(extraction_parameters["types_PostgreSQL_to_exclude"])

if os.path.exists(output_dir):
    if not os.path.isdir(output_dir):
        raise NotADirectoryError(f"Output path {output_dir} does not refer to a directory.")
    if os.listdir(output_dir):
        raise Exception(f"Output directory {output_dir} should be empty")
else:
    os.mkdir(output_dir)


# Create a connection to the database
logging.debug("Retrieve credentials ...")
connection_parameters: dict = parameters["database"]
host: str = connection_parameters["host"]
database_name: str = connection_parameters["database_name"]
port: int = connection_parameters["port"]
username: str = connection_parameters["username"]

password: str = getpass.getpass(f"Username {username}, please enter your password:")
logging.debug("Retrieve credentials [OK]")

start_time: float = time.time()

logging.debug("Creating sqlalchemy engine to fetch tables ...")
engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
    f'postgresql://{username}:{password}@{host}:{port}/{database_name}',
    isolation_level="SERIALIZABLE"
)
logging.debug("Creating sqlalchemy engine to fetch tables [OK]")

# fetch list of all PostgreSQL tables to extract with their schema name, using fetch_tables_to_extract.sql
with open("fetch_tables_to_extract.sql", "r") as request_file:
    request_fetch_tables_to_extract: str = request_file.read().replace("\n", " ")

with engine.connect() as connection:
    logging.debug("Fetching table names ...")
    with connection.begin():
        result_table_names: sqlalchemy.engine.cursor.CursorResult = connection.execute(
            sqlalchemy.text(request_fetch_tables_to_extract)
        )
        # build dataframe to store tables and schemas names
        df_table_names_postgres: pd.DataFrame = pd.DataFrame(result_table_names.fetchall(),
                                                             columns=list(result_table_names.keys()))


for index_table in df_table_names_postgres.index:
    table_name_postgres: str = df_table_names_postgres.loc[index_table, "table_name"]
    table_schema_postgres: str = df_table_names_postgres.loc[index_table, "table_schema"]

    # retrieve columns to load, exclude or cast to string in DuckDB
    logging.debug(f"Loading columns and types to fetch table {table_schema_postgres}.{table_name_postgres} ...")
    with engine.connect() as connection:
        with connection.begin():
            table_columns_and_types: sqlalchemy.engine.cursor.CursorResult = connection.execute(
                sqlalchemy.text(f"SELECT column_name, data_type "
                                f"FROM information_schema.columns "
                                f"WHERE table_schema = '{table_schema_postgres}' "
                                f"and table_name = '{table_name_postgres}' "
                                f"ORDER BY column_name")
            )

    columns_to_load: list[str] = []
    columns_to_load_with_cast: list[str] = []
    for column_name, data_type in table_columns_and_types.fetchall():
        if data_type not in types_postgres_to_exclude:
            columns_to_load.append(column_name)
            if data_type in types_to_cast_to_string:
                columns_to_load_with_cast.append(f"CAST({column_name} AS VARCHAR) AS {column_name}")
            else:
                columns_to_load_with_cast.append(column_name)

    logging.debug(f"Loading columns and types to fetch table {table_schema_postgres}.{table_name_postgres} [OK]")

    logging.debug(f"Fetching table {table_schema_postgres}.{table_name_postgres} ...")
    try:
        # copy table from PostgreSQL to DuckDB in-memory database and cast the types
        duckdb.sql(
            f"CREATE TABLE extraction_{table_name_postgres} AS "
            f"SELECT {', '.join(columns_to_load_with_cast)} "
            f"FROM postgres_scan('dbname={database_name} host={host} port={port} "
            f"user={username} password={password}', "
            f"'{table_schema_postgres}', '{table_name_postgres}')"
        )
        logging.debug(f"Fetching table {table_schema_postgres}.{table_name_postgres} [OK]")

        logging.debug(f"Writing table {table_schema_postgres}.{table_name_postgres} to parquet files ...")

        output_path: str = os.path.join(output_dir, f"{table_schema_postgres}.{table_name_postgres}")
        duckdb.sql(
            f"COPY ("
            f"SELECT {', '.join(columns_to_load)}, "
            f"FROM extraction_{table_name_postgres} "
            f") TO '{output_path}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, OVERWRITE_OR_IGNORE 1)"
        )

        # delete table from memory
        duckdb.sql(
            f"DROP TABLE extraction_{table_name_postgres}"
        )
        logging.debug(f"Writing table {table_schema_postgres}.{table_name_postgres} to parquet files [OK]")
    except (duckdb.ConversionException, duckdb.IOException) as e:
        # duckdb.ConversionException can occur for example when converting timestamp columns
        # duckdb.IOException can occur, for example when reading interval
        # So these two types should be either cast to string or excluded
        logging.warning(e)
        logging.warning(f"Table {table_schema_postgres}.{table_name_postgres} could not be extracted")
        continue

total_time: float = time.time() - start_time

logging.info(f"Extracted {len(df_table_names_postgres)} tables, total time {total_time} seconds")

with open("DuckDB/exec_time.txt", "w") as f:
    f.write(f"{total_time}")
