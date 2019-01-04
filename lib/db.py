"""Common functions for dealing with database connections."""

from os import fspath, remove
from os.path import abspath, exists
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime
import pandas as pd
from lib.util import log

VERSION = 'v0.1.1'

PROCESSED = Path('data') / 'processed'
DB_FILE = abspath(PROCESSED / 'ectoparasites.sqlite.db')
SCRIPT_PATH = Path('sql')

SITE_COLUMNS = """site_id dataset_id long lat radius elevation elevation_radius
    country road_location dataset_site_id geohash site_json""".split()

HOST_COLUMNS = """host_id site_id host_taxon_id dataset_id mass age sex
    dataset_host_id host_json""".split()

SAMPLE_COLUMNS = """sample_id host_id dataset_id sample_date vial_no method
    ectoparasites ectoprocessed box_location box_id dataset_sample_id method
    sample_json""".split()

PARASITE_COLUMNS = """parasite_id sample_id parasite_taxon_id dataset_id
    verbatim_abundance abundance locality_tube vial_no dna_extracted dna_method
    pathogen_dna_id species_slide parasite_json""".split()


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

    insert_db_version()


def insert_db_version():
    """Insert the version of the database."""
    cxn = connect()
    cxn.execute('INSERT INTO version (version) VALUES (?)', (VERSION, ))
    cxn.commit()


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

    sql = """DELETE FROM sites
              WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)"""
    cxn.execute(sql)

    sql = """DELETE FROM hosts
              WHERE site_id NOT IN (SELECT site_id FROM sites)"""
    cxn.execute(sql)

    sql = """DELETE FROM samples
              WHERE host_id NOT IN (SELECT host_id FROM hosts)"""
    cxn.execute(sql)

    sql = """DELETE FROM parasite_groups
              WHERE sample_id NOT IN (SELECT sample_id FROM samples)"""
    cxn.execute(sql)

    sql = """DELETE FROM parasites
              WHERE parasite_group_id
                NOT IN (SELECT parasite_group_id FROM parasite_groups)"""
    cxn.execute(sql)

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
