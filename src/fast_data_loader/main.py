import importlib.metadata
from typing import Annotated
from fast_data_loader import scanner, utils
import os
import sys 

import logging
import psycopg
import typer
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

app = typer.Typer()


@app.command()
def check(directory: Path):
    logging.info("Getting table sync status")
    unloaded = scanner.check_unloaded_tables(directory)

    if unloaded:
        logging.warning("Local files that are not present on server:")
        for file in unloaded:
            logging.warning(f" {file.name}")
    
    unaware = set(scanner.get_aware_remote_tables()).difference(scanner.get_remote_tables())

    if unaware:
        logging.warning("Files are present on the server that were not added with the fast_data_tool.")
        logging.warning("Affected tables:")
        for file in unaware:
            logging.warning(f" {file}")
    

@app.command()
def version():
    print(importlib.metadata.version("fast_data_loader"))


@app.command()
def debug():
    logging.info("Checking database connectivity...")
    with psycopg.connect("") as conn:
        cur = conn.cursor()
        result = cur.execute("SELECT version();").fetchone()
        logging.info(" Connectivity OK")
        logging.info(f"  {result[0]}")


@app.command()
def init():
    with psycopg.connect("") as conn:
        logging.info("Check `source` schema exists...")
        source_schema_exists = conn.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'source');"
        ).fetchone()[0]
        if source_schema_exists:
            logging.info("Schema `source` exists")
        else:
            logging.warning("Schema `source` does not exist. Creating...")
            conn.execute("CREATE SCHEMA source;")
            logging.info("Schema `source` created")

        logging.info("Check state table exists")
        data_state_table_exists = conn.execute("""
                SELECT EXISTS (SELECT
                            FROM information_schema.tables
                            WHERE table_schema = 'source'
                                AND table_name = '_data_load_state');
                """).fetchone()[0]
        if data_state_table_exists:
            logging.info("State table exists")
        else:
            logging.warning("State table does not exist, creating...")
            conn.execute("""CREATE TABLE source._data_load_state (
                    file_name     varchar   not null,
                    load_datetime timestamp not null,
                    file_hash_md5 varchar   not null
                );""")

@app.command()
def push(directory: Path, force: Annotated[bool, typer.Option("--force")] = False):
    logging.info("Generating plan...")

    unloaded = scanner.check_unloaded_tables(directory)

    if not unloaded:
        logging.info("No work to do")
        sys.exit(0)

    logging.info(" === PLAN === ")
    for file in unloaded:
        logging.info(f"  PUSH {file.name} --> {os.environ['PGDATABASE']}.source.{scanner.generate_table_name(file)}")
    
    if not force:
        print("")
        user_option = input("Proceed? (y/N) > ")
        if user_option != 'y':
            sys.exit(0)
    else:
        logging.info("")
        logging.info(" Force flag specified, proceeding...")
    
    for file in unloaded:
        logging.info(f"COPY {file.name}")
        utils.upload_file(file, scanner.generate_table_name(file))



if __name__ == "__main__":
    app()
