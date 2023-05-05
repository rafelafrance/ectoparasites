#!/usr/bin/env python3

import argparse
import logging
import sqlite3
import textwrap
from pathlib import Path
from typing import Literal

import pandas as pd
from pylib import log, tables


def main():
    log.started()

    args = parse_args()

    csv_tables = get_csv_tables(args.csv_dir)

    ingest_data(args.db, csv_tables, args.replace)

    log.finished()


def ingest_data(db_path, csv_tables, replace):
    if_exists: Literal["replace", "append"] = "append"
    if replace:
        if_exists = "replace"

    with sqlite3.connect(db_path) as cxn:
        for table in tables.TABLES:
            logging.info(f"Getting data for: {table.name}")
            df = extract_csv_data(csv_tables, table)
            if not df.empty:
                df.to_sql(table.name, con=cxn, index=False, if_exists=if_exists)


def get_csv_tables(csv_dir):
    csv_tables = {}
    for path in sorted(csv_dir.glob("*.csv")):
        df = pd.read_csv(path)
        csv_tables[path.stem.lower()] = fix_column_names(df)
    return csv_tables


def fix_column_names(df):
    """Normalize column names to reduce the amount of CSV columns to search."""
    renames = {c: c.lower() for c in df.columns}
    df = df.rename(renames, axis="columns")
    return df


def extract_csv_data(csv_tables, db_table):
    all_csv_data = []

    # Look thru all CSV data frames
    for csv_name, csv_table in csv_tables.items():
        csv_column_set = set(csv_table.columns)

        best_missing = csv_column_set

        # Examine every possible combination of column names for a match
        for targets in db_table.csv_permutations():
            target_set = set(targets)

            missing = target_set - set(csv_table.columns)
            if len(missing) < len(best_missing):
                best_missing = missing

            # Did we find all table's required columns in the CSV?
            if csv_column_set >= target_set:

                db_table_data = csv_table.loc[:, targets]

                # Rename CSV columns to what they'll be in the database table
                renames = {t: f.name for t, f in zip(targets, db_table.columns)}
                db_table_data = db_table_data.rename(renames, axis="columns")

                # Try to get the proper data type for each column
                types = {f.name: tables.SQLITE_TYPE[f.type] for f in db_table.columns}
                db_table_data = db_table_data.astype(types, errors="ignore")

                all_csv_data.append(db_table_data)
                logging.info(f"Hit  {db_table.name} & {csv_name}")
                break  # Go look for data in the next CSV table
        else:
            logging.info(f"Miss {db_table.name} & {csv_name} Missing = {best_missing}")

    if all_csv_data:
        table_df = pd.concat(all_csv_data)
        table_df = table_df.drop_duplicates()
    else:
        table_df = pd.DataFrame()

    return table_df


def parse_args():
    description = """Ingest ectoparasite data."""

    parser = argparse.ArgumentParser(
        description=textwrap.dedent(description),
        allow_abbrev=True,
        fromfile_prefix_chars="@",
    )

    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        metavar="PATH",
        help="""Use this ectoparasite DB.""",
    )

    parser.add_argument(
        "--csv-dir",
        type=Path,
        required=True,
        metavar="DIR",
        help="""Input CSVs are here.""",
    )

    parser.add_argument(
        "--replace",
        action="store_true",
        help="""Are we appending data to the tables or overwriting the tables.""",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
