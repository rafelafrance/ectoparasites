#!/usr/bin/env python

"""Wrapper for ecoparasites functions."""

import argparse
import lib.db as db
from lib.util import log
import lib.columbia_ingest as columbia_ingest


SEPARATOR = '*****************************************'

INGESTS = [
    ('columbia', columbia_ingest)]
INGEST_OPTIONS = [i[0] for i in INGESTS]


def parse_args():
    """Get user input."""
    parser = argparse.ArgumentParser(
        allow_abbrev=True,
        description='Extract, transform, & load data for the '
                    'sightings database.')
    subparsers = parser.add_subparsers()

    backup_parser = subparsers.add_parser(
        'backup',
        help="""Backup the SQLite3 database.""")
    backup_parser.set_defaults(func=backup)

    create_parser = subparsers.add_parser(
        'create',
        help="""Create the SQLite3 database tables & indices.""")
    create_parser.set_defaults(func=create)

    ingest_parser = subparsers.add_parser(
        'ingest',
        help=f"""Ingest a dataset into the SQLite3 database.""")
    ingest_parser.add_argument(
        'datasets',
        nargs='+',
        metavar='DATASET',
        choices=INGEST_OPTIONS,
        help=f"""Ingest a dataset into the SQLite3 database.
            Options: { ", ".join(INGEST_OPTIONS) }.""")
    ingest_parser.set_defaults(func=ingest)

    return parser.parse_args()


def backup(args):
    """Backup the SQLite3 database."""
    db.backup_database()


def create(args):
    """Create the SQLite3 database."""
    db.create()


def ingest(args):
    """Ingest datasets into the SQLite3 database."""
    for ingest, module in INGESTS:
        if ingest in args.datasets:
            log(SEPARATOR)
            module.ingest()
    log(SEPARATOR)


def ectoparasites():
    """Do the selected actions."""
    args = parse_args()
    if 'func' in args:
        args.func(args)
    log('Done')


if __name__ == '__main__':
    ectoparasites()
