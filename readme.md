# PostgreSQL-extraction

A tool to extract a PostgreSQL database or a part of it and create a duckdb database dump and the set of corresponding parquet files.

## Environment

With `conda` installed, create the environment with:
```
conda env create -f environment.yml
```
Activate the environment with:
```
conda activate PostgreSQL-extraction
```

## Install DuckDB extension for PostgreSQL

Try to install the extension `postgres_scanner` directly with:
```
python install_postgres_scanner_directly.py
```

If direct installation does not work (which is the case for DuckDB version `0.8.1`):
- Download http://extensions.duckdb.org/v0.8.1/windows_amd64/postgres_scanner.duckdb_extension.gz (adapt version to DuckDB, use `pip show duckdb` to retrieve your DuckDB version)
- Unzip the file `postgres_scanner.duckdb_extension.gz` and place it in the working directory
- Run `python install_postgres_scanner_from_file.py`
