"""Ingest data from the Columbia expedition."""

from pathlib import Path
import pandas as pd
import lib.db as db
from lib.util import log


DATASET_ID = 'columbia_01'
RAW_DIR = Path('data') / 'raw'
DATA_CSV = RAW_DIR / 'ectoparasitesColombia.example.csv'


def ingest():
    """Ingest the data."""
    raw_data = get_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'description': 'Columbia exctoparasites expedition 01',
        'version': '2018-11-04'})

    to_taxon_id = insert_host_taxa(raw_data)


def get_raw_data():
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')
    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')
    raw_data['sci_name'] = raw_data['species'].str.split().str.join(' ')
    return raw_data


def insert_host_taxa(raw_data):
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} taxa')

    firsts = raw_data.sci_name.duplicated(keep='first')
    taxa = raw_data.loc[firsts, ['sci_name', 'family', 'order']].copy()

    taxa['genus'] = taxa.sci_name.str.split().str[0]
    taxa['group'] = 'birds'

    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)

    cxn = db.connect()
    taxa.to_sql('taxa', cxn, if_exists='append', index=False)

    sql = """SELECT sci_name, taxon_id FROM taxa"""
    return pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()


if __name__ == '__main__':
    ingest()
