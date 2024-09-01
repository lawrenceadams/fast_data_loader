from pathlib import Path
import psycopg

def generate_table_name(filename: Path) -> str:
    return filename.stem

def get_remote_tables() -> list[str]:
    with psycopg.connect("") as conn:
        remote_tables = conn.execute("""select table_name
            from information_schema.tables
            where table_schema = 'source';""").fetchall()
        if remote_tables != []:
            return [table[0] for table in remote_tables]
        return []

def get_aware_remote_tables() -> tuple[str]:
    with psycopg.connect("") as conn:
        data_state_tables = conn.execute("""select file_name
            from source._data_load_state;""").fetchall()
        
        if len(data_state_tables) > 0:
            return [file for file in data_state_tables[0]]
        return []
    
def check_unloaded_tables(directory: Path) -> list[Path]:
    files = {f.stem: f for f in directory.glob("*.parquet")}
    keys = files.keys()
    unloaded = list(set(keys).difference(get_remote_tables()))

    return [files[key] for key in unloaded]