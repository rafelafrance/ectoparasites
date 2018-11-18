"""Ingest data from the Columbia expedition."""

import re
from pathlib import Path
import pandas as pd
import lib.db as db
from lib.util import log, json_object


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
    to_site_id = insert_sites(raw_data)
    to_host_id = insert_hosts(raw_data, to_site_id)
    to_sample_id = insert_samples(raw_data, to_host_id)
    insert_parasites(raw_data, to_sample_id, to_taxon_id)


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

    has_name = taxa.sci_name.notna()
    taxa = taxa.loc[has_name, :].copy()

    taxa['genus'] = taxa.sci_name.str.split().str[0]
    taxa['group'] = 'birds'

    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)

    cxn = db.connect()
    taxa.to_sql('taxa', cxn, if_exists='append', index=False)

    sql = """SELECT sci_name, taxon_id FROM taxa"""
    return pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()


def insert_sites(raw_data):
    """Insert sites."""
    log(f'Inserting {DATASET_ID} sites')

    sites = raw_data.loc[:, ['dpto', 'locality', 'elevation']]
    sites = sites.drop_duplicates().fillna('').copy()

    sites['site_id'] = db.get_ids(sites, 'sites')
    sites['dataset_id'] = DATASET_ID
    sites['country'] = 'Columbia'
    sites['verbatim_elevation'] = sites.elevation
    sites['elevation_radius'] = ''
    sites[['elevation', 'elevation_radius']] = sites.apply(
        get_elevation, axis='columns')

    fields = """dpto locality verbatim_elevation""".split()
    sites['site_json'] = json_object(sites, fields)

    fields = """site_id dataset_id elevation elevation_radius
        country site_json""".split()
    sites.loc[:, fields].to_sql(
        'sites', db.connect(), if_exists='append', index=False)

    # Build dictionary to map sites to site IDs
    return sites.set_index(['locality', 'elevation']).site_id.to_dict()


def get_elevation(row):
    """Get the elevation and elevation radius from the verbatim elevation."""
    parts = [re.sub(r'\D', '', p) for p in row.elevation.split('-')]

    if len(parts) > 1:
        radius = (float(parts[1]) - float(parts[0])) / 2
        elevation = float(parts[0]) + radius
    elif parts[0]:
        elevation = float(parts[0])
        radius = None
    else:
        elevation = None
        radius = None

    return pd.Series([elevation, radius])


def insert_hosts(raw_data, to_site_id):
    """Insert hosts."""
    log(f'Inserting {DATASET_ID} hosts')


def insert_samples(raw_data, to_host_id):
    """Insert samples."""
    log(f'Inserting {DATASET_ID} samples')


def insert_parasites(raw_data, to_sample_id, to_taxon_id):
    """Insert parasites."""
    log(f'Inserting {DATASET_ID} parasites')


if __name__ == '__main__':
    ingest()
