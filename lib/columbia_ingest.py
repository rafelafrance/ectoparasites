"""Ingest data from the Columbia expedition."""

import re
import json
from pathlib import Path
import pandas as pd
import lib.db as db
from lib.util import log, json_object


DATASET_ID = 'columbia_01'
RAW_DIR = Path('data') / 'raw'
DATA_XLSX = RAW_DIR / 'Colombia - 2015 - 2016part.xlsx'
DATA_SHEET = 'up through 2015'

SITE_KEYS = 'Latitude_Original Longitude_Original verbatim_elevation'.split()


def ingest():
    """Ingest the data."""
    raw_data = read_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'description': 'Columbia exctoparasites expedition 01',
        'version': '2018-11-04'})

    to_taxon_id = insert_taxa(raw_data)
    to_site_id = insert_sites(raw_data)
    insert_hosts(raw_data, to_site_id, to_taxon_id)
    insert_samples(raw_data)
    insert_parasites(raw_data, to_taxon_id)


def read_raw_data():
    """Get the raw data."""
    log(f'Reading {DATASET_ID} raw data')
    converters = {c: str for c
                  in pd.read_excel(DATA_XLSX, sheet_name=DATA_SHEET).columns}

    raw_data = pd.read_excel(
        DATA_XLSX, sheet_name=DATA_SHEET, converters=converters)
    raw_data = raw_data.rename(columns={'Genus': 'genus'})
    raw_data['dataset_id'] = DATASET_ID

    return raw_data


def insert_taxa(raw_data):
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} host taxa')

    raw_data['binary'] = raw_data.genus + ' ' + raw_data.Species
    raw_data['binary'] = raw_data.binary.str.split().str.join(' ')
    raw_data.subspecies = raw_data.subspecies.fillna('')
    raw_data['sci_name'] = raw_data.binary + ' ' + raw_data.subspecies
    raw_data['sci_name'] = raw_data.sci_name.str.split().str.join(' ')

    firsts = raw_data.sci_name.duplicated(keep='first')

    fields = 'sci_name binary order genus'.split()
    taxa = raw_data.loc[~firsts, fields].copy()

    has_name = taxa.sci_name.notna()
    taxa = taxa.loc[has_name, :].copy()

    taxa['family'] = ''
    taxa['group'] = 'host'

    parasites = []
    for group in 'lice feather_mites ticks flies fleas others'.split():
        parasites.append({
            'taxon_id': None,
            'sci_name': group,
            'binary': '',
            'group': group,
            'order': '',
            'family': '',
            'genus': ''})
    taxa = taxa.append(parasites, ignore_index=True, sort=True)

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

    raw_data['verbatim_elevation'] = raw_data['elevation'].astype(str)

    fields = """Country Departament Municipio Locality vereda reserva
        Latitude_Original Longitude_Original Longitude Latitude
        verbatim_elevation dataset_id""".split()

    sites = raw_data.loc[:, fields]
    sites = sites.drop_duplicates(SITE_KEYS).copy().rename(columns={
        'Country': 'country', 'Latitude': 'lat', 'Longitude': 'long'})

    sites['site_id'] = db.get_ids(sites, 'sites')
    sites['geohash'] = None
    sites['road_location'] = None
    sites['dataset_site_id'] = None

    sites.lat = pd.to_numeric(sites.lat, errors='coerce')
    sites.long = pd.to_numeric(sites.long, errors='coerce')
    sites['radius'] = None

    sites['elevation_radius'] = ''
    sites['verbatim_elevation'] = sites['verbatim_elevation'].astype(str)
    sites[['elevation', 'elevation_radius']] = sites.apply(
        get_elevation, axis='columns')

    fields = """Departament Municipio Locality vereda reserva
        Latitude_Original Longitude_Original verbatim_elevation""".split()
    sites['site_json'] = json_object(sites, fields)

    sites.loc[:, db.SITE_COLUMNS].to_sql(
        'sites', db.connect(), if_exists='append', index=False)

    # Build dictionary to map sites to site IDs
    return sites.set_index(SITE_KEYS).site_id.to_dict()


def get_elevation(row):
    """Get the elevation and elevation radius from the verbatim elevation."""
    parts = [re.sub(r'\D', '', p) for p in row.verbatim_elevation.split('-')]

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


def insert_hosts(raw_data, to_site_id, to_taxon_id):
    """Insert hosts."""
    log(f'Inserting {DATASET_ID} hosts')

    raw_data['host_id'] = db.get_ids(raw_data, 'hosts')

    raw_data['site_key'] = tuple(zip(
        raw_data.Latitude_Original,
        raw_data.Longitude_Original,
        raw_data.verbatim_elevation))
    raw_data['site_id'] = raw_data.site_key.map(to_site_id)

    raw_data['host_taxon_id'] = raw_data.sci_name.map(to_taxon_id)
    raw_data['mass'] = raw_data['mass (g)']
    raw_data['age'] = None
    raw_data['dataset_host_id'] = None

    fields = """needs_confirmation""".split()
    raw_data['host_json'] = json_object(raw_data, fields)

    raw_data.loc[:, db.HOST_COLUMNS].to_sql(
        'hosts', db.connect(), if_exists='append', index=False)


def insert_samples(raw_data):
    """Insert samples."""
    log(f'Inserting {DATASET_ID} samples')

    raw_data['sample_id'] = db.get_ids(raw_data, 'samples')
    raw_data['sample_date'] = raw_data['date']
    raw_data['method'] = raw_data['Ectoparsite Sampling Method']
    raw_data['ectoparasites'] = raw_data['Ectoparasite_sample']
    raw_data['ectoprocessed'] = None
    raw_data['box_location'] = \
        raw_data['Ecotparasite Box Location (Cold Storage)']
    raw_data['box_id'] = raw_data['ectoparasites_box']
    raw_data['vial_no'] = None
    raw_data['dataset_sample_id'] = raw_data['field_number']

    raw_data = raw_data.rename(columns={'Collector (PI)': 'Collector_PI'})
    fields = ['Collector_PI']
    raw_data['sample_json'] = json_object(raw_data, fields)

    raw_data.loc[:, db.SAMPLE_COLUMNS].to_sql(
        'samples', db.connect(), if_exists='append', index=False)


def insert_parasites(raw_data, to_taxon_id):
    """Insert parasites."""
    log(f'Inserting {DATASET_ID} parasites')

    parasite_records = []
    for _, row in raw_data.iterrows():

        parasite_records.append(parasite_record(
            row, to_taxon_id['lice'], 'lice_total',
            ['Ecomorph Notes', 'lice ages']))

        parasite_records.append(parasite_record(
            row, to_taxon_id['feather_mites'], 'feather_mites_total'))

        parasite_records.append(parasite_record(
            row, to_taxon_id['ticks'], 'ticks_total'))

        parasite_records.append(parasite_record(
            row, to_taxon_id['flies'], 'flies_total'))

        parasite_records.append(
            parasite_record(row, to_taxon_id['fleas'], 'fleas_total'))

        parasite_records.append(
            parasite_record(row, to_taxon_id['others'], 'others'))

    parasites = pd.DataFrame(parasite_records)
    parasites['parasite_id'] = db.get_ids(parasites, 'parasites')

    parasites.loc[:, db.PARASITE_COLUMNS].to_sql(
        'parasites', db.connect(), if_exists='append', index=False)


def parasite_record(
        row, parasite_taxon_id, verbatim_abundance, parasite_json=[]):
    """Insert one parasite record."""
    row = row.to_dict()

    record = {
        'sample_id': row['sample_id'],
        'parasite_taxon_id': parasite_taxon_id,
        'dataset_id': DATASET_ID,
        'verbatim_abundance': row[verbatim_abundance],
        'locality_tube': None,
        'vial_no': None,
        'dna_extracted': None,
        'dna_method': None,
        'pathogen_dna_id': None,
        'species_slide': None}

    abundance = re.sub(r'\D', '', str(row[verbatim_abundance]))
    record['abundance'] = int(abundance) if abundance else 0

    record['parasite_json'] = {}
    for field in parasite_json:
        record['parasite_json'][field] = row[field]
    record['parasite_json'] = json.dumps(record['parasite_json'])

    return record


if __name__ == '__main__':
    ingest()
