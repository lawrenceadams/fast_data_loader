import hashlib
import logging
from pathlib import Path

import duckdb


def upload_file(file_path: Path, table_name: str):
    hash = calculate_md5(file_path)
    logging.info(f" {file_path.name} -> {hash}")

    with duckdb.connect() as duck_conn:
        query = f"""
        ATTACH '' AS remote (TYPE POSTGRES);
        CREATE TABLE remote.source.{table_name} AS (SELECT * FROM '{file_path.absolute()}');

        
        INSERT INTO remote.source._data_load_state (file_name, load_datetime, file_hash_md5)
            VALUES ('{file_path.name}', NOW(), '{hash}');
        """

        duck_conn.execute(query)


def calculate_md5(file: Path) -> str:
    with open(file, "rb") as handle:
        return hashlib.md5(handle.read()).hexdigest()
