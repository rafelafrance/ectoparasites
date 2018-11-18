"""Common functions for dealing with database connections."""

from os import fspath, remove
from os.path import abspath, exists
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime
import pandas as pd
from lib.util import log


PROCESSED = Path('data') / 'processed'
DB_FILE = abspath(PROCESSED / 'ectoparasites.sqlite.db')
SCRIPT_PATH = Path('sql')

SITE_COLUMNS = """site_id dataset_id long lat radius elevation elevation_radius
    country dataset_site_id geohash site_json""".split()


def connect(path=None):
    """Connect to an SQLite database."""
    path = path if path else str(DB_FILE)
    cxn = sqlite3.connect(path)

    cxn.execute('PRAGMA page_size = {}'.format(2**16))
    cxn.execute('PRAGMA busy_timeout = 10000')
    cxn.execute('PRAGMA synchronous = OFF')
    cxn.execute('PRAGMA journal_mode = OFF')
    return cxn


def create():
    """Create the database."""
    log(f'Creating database')

    script = fspath(SCRIPT_PATH / 'create_db.sql')
    cmd = f'sqlite3 {DB_FILE} < {script}'

    if exists(DB_FILE):
        remove(DB_FILE)

    subprocess.check_call(cmd, shell=True)


def backup_database():
    """Backup the SQLite3 database."""
    log('Backing up SQLite3 database')
    now = datetime.now()
    backup = f'{DB_FILE[:-3]}_{now.strftime("%Y-%m-%d")}.db'
    cmd = f'cp {DB_FILE} {backup}'
    subprocess.check_call(cmd, shell=True)


def insert_dataset(dataset):
    """Insert the DB verion."""
    cxn = connect()
    sql = """INSERT INTO datasets (dataset_id, description, version)
                  VALUES (:dataset_id, :description, :version)"""
    cxn.execute(sql, dataset)
    cxn.commit()


def delete_dataset(dataset_id):
    """Clear dataset from the database."""
    log(f'Deleting old {dataset_id} records')

    cxn = connect()
    cxn.execute('DELETE FROM datasets WHERE dataset_id = ?', (dataset_id, ))
    cxn.execute('DELETE FROM sites WHERE dataset_id = ?', (dataset_id, ))
    cxn.execute('DELETE FROM hosts WHERE dataset_id = ?', (dataset_id, ))
    cxn.execute('DELETE FROM samples WHERE dataset_id = ?', (dataset_id, ))
    cxn.execute('DELETE FROM parasites WHERE dataset_id = ?', (dataset_id, ))
    cxn.commit()


def get_dataset_id(description):
    """Get the dataset ID via the dataset's name."""


def get_ids(df, table):
    """Get IDs to add to the dataframe."""
    start = next_id(table)
    return range(start, start + df.shape[0])


def next_id(table):
    """Get the max value from the table's ID field."""
    cxn = connect()
    if not table_exists(cxn, table):
        return 1
    field = 'taxon_id' if table == 'taxa' else table[:-1] + '_id'
    sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
    return cxn.execute(sql).fetchone()[0] + 1


def table_exists(cxn, table):
    """Check if a table exists."""
    sql = """
        SELECT COUNT(*) AS n
          FROM sqlite_master
         WHERE "type" = 'table'
           AND name = ?"""
    results = cxn.execute(sql, (table, ))
    return results.fetchone()[0]


def drop_duplicate_taxa(taxa):
    """Remove taxa already in the database from the data frame."""
    cxn = connect()
    existing = pd.read_sql('SELECT sci_name, taxon_id FROM taxa', cxn)
    existing = existing.set_index('sci_name').taxon_id.to_dict()
    in_existing = taxa.sci_name.isin(existing)
    return taxa.loc[~in_existing, :].drop_duplicates('sci_name').copy()
