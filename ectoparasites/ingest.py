#!/usr/bin/env python3

import argparse
import logging
import sqlite3
import textwrap
from dataclasses import dataclass
from itertools import product
from pathlib import Path

import pandas as pd
from pylib import log


@dataclass
class Field:
    name: str
    type: str
    columns: list[str]


@dataclass
class Table:
    name: str
    fields: list[Field]


TYPE = {
    "categorical": "string", 
    "int": "int32",
    "numeric": "float32",
    "numerical": "float32",
    "text":  "string",
    "y/n": "boolean",
}


TABLES = [
    Table("taxonomy", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(
            name="species",
            type="text",
            columns=["species", "bird species", "specie", "bird_specie"],
        ),
    ]),
    Table("gps", [
        Field(name="site", type="categorical", columns=["site"]),
        Field(name="location", type="categorical", columns=["location", "localidad"]),
        Field(name="latitude", type="numeric", columns=["lat"]),
        Field(name="longitude", type="numeric", columns=["lon"]),
        Field(name="altitude", type="numeric", columns=["ele"]),
    ]),
    Table("site", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(name="site", type="categorical", columns=["site"]),
        Field(name="location", type="categorical", columns=["location", "localidad"]),
    ]),
    Table("date", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(name="day", type="int", columns=["day"]),
        Field(name="month", type="text", columns=["month"]),
        Field(name="year", type="int", columns=["year"]),
    ]),
    Table("bird_quantitative", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(name="mass", type="categorical", columns=["mass"]),
        Field(name="tar", type="numerical", columns=["tar"]),
        Field(name="ct", type="numerical", columns=["ct"]),
        Field(name="ce", type="numerical", columns=["ce"]),
        Field(name="pw", type="numerical", columns=["pw"]),
        Field(name="ph", type="numerical", columns=["ph"]),
        Field(name="com", type="numerical", columns=["com"]),
        Field(name="halux", type="numerical", columns=["hal"]),
        Field(name="nail", type="numerical", columns=["uña", "una"]),
        Field(name="claw_extension", type="numerical", columns=["ext"]),
        Field(name="rc", type="numerical", columns=["rc"]),
        Field(name="re", type="numerical", columns=["re"]),
        Field(name="wing", type="numerical", columns=["ala"]),
        Field(name="p_s", type="numerical", columns=["p-s"]),
    ]),
    Table("bird_categorical", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(name="age", type="categorical", columns=["age"]),
        # Field(name="how_aged", type="categorical", columns=["how aged"]),
        Field(name="sex", type="categorical", columns=["sex"]),
        # Field(name="how_sexed", type="categorical", columns=["how sexed"]),
        Field(name="reproduction", type="categorical", columns=["rep", "rep."]),
        Field(name="fat", type="categorical", columns=["fat"]),
        Field(name="bm", type="categorical", columns=["bm"]),
        Field(name="fm", type="categorical", columns=["fm"]),
        # Field(name="pec_muscle", type="categorical", columns=["musculo"]),
        # Field(name="pro_cloacal", type="categorical", columns=["pro_cloacal"]),
        # Field(name="brood_patch", type="categorical", columns=["parche_inc"]),
        Field(name="recapture", type="y/n", columns=["recap"]),
        # Field(name="status", type="categorical", columns=["status"]),
        # Field(name="leg_color", type="categorical", columns=["leg color"]),
        # Field(name="orbital_color", type="categorical", columns=["orbital color"]),
        # Field(name="skull", type="categorical", columns=["skull"]),
        Field(name="notes", type="categorical", columns=["notes", "observaciones"]),
    ]),
    Table("bird_samples", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        Field(name="blood", type="y/n", columns=["blood"]),
        Field(name="ectos", type="y/n", columns=["paras"]),
        Field(name="feather", type="y/n", columns=["feather"]),
        # Field(name="collect_number", type="numerical", columns=["num_colecta"]),
        Field(name="photo", type="y/n", columns=["photo#", "photo"]),
    ]),
    Table("bird_band", [
        Field(name="capture_id", type="categorical", columns=["id"]),
        Field(name="band", type="categorical", columns=["band"]),
        # Field(name="ring_color", type="categorical", columns=["color_anillo"]),
        # Field(name="bander", type="text", columns=["anillador"]),
        # Field(name="banding_day", type="categorical", columns=["banding day"]),
    ]),
    # Table("net_location", [
    #     Field(name="capture_id", type="categorical", columns=["id"]),
    #     Field(name="band", type="categorical", columns=["band"]),
    #     Field(name="site", type="categorical", columns=["site"]),
    #     Field(name="location", type="categorical", columns=["location"]),
    #     # Field(name="net_number", type="categorical", columns=["net"]),
    # ]),
    # Table("positive_ectos", [
    #     Field(name="capture_id", type="categorical", columns=["id"]),
    #     Field(name="box", type="", columns=["box"]),
    #     Field(name="box_position", type="", columns=["box_position"]),
    #     Field(name="bird_specie", type="", columns=["bird_specie"]),
    #     Field(name="ecto_group", type="", columns=["ecto_group"]),
    #     Field(name="station", type="", columns=["station"]),
    #     Field(name="capture_id", type="", columns=["capture_id"]),
    #     Field(name="date", type="", columns=["date"]),
    #     Field(name="format_date", type="", columns=["format_date"]),
    #     Field(name="year", type="", columns=["year"]),
    #     Field(name="month", type="", columns=["month"]),
    #     Field(name="day", type="", columns=["day"]),
    #     Field(name="band", type="", columns=["band"]),
    #     Field(name="old_box_position", type="", columns=["old_box_position"]),
    #     Field(name="notes", type="", columns=["notes"]),
    # ]),
    # Table("ectos", [
    #     Field(name="id", type="categorical", columns=["id"]),
    #     Field(name="band", type="categorical", columns=["band"]),
    #     Field(name="ectos", type="y/n", columns=["ectos"]),
    #     Field(name="ecto_type", type="categorical", columns=["ecto_type"]),
    #     Field(name="looked_ectos", type="y/n", columns=["looked_ectos"]),
    #   Field(name="ectos_technique", type="categorical", columns=["ectos_technique"]),
    # ]),
    # Table("dataset", [
    #     Field(name="dataset_id", type="text", columns=[]),  Auto generate
    #     Field(name="principle_investigator", type="categorical", columns=[]), Literal
    #     Field(name="data_type", type="categorical", columns=[]), Literal
    #     Field(name="country", type="categorical", columns=[]), Literal
    # ]),
    # Table("site", [
    # ]),
    # Table("locality", [
    #     Field(name="site_id", type="text", columns=["site_id"]),
    #     Field(name="location_id", type="categorical", columns=["location", "localidad"]),
    #     Field(name="latitude", type="numeric", columns=["lat"]),
    #     Field(name="longitude", type="numeric", columns=["lon"]),
    #     Field(name="elevation", type="numeric", columns=["ele"]),
    # ]),
]


def main():
    log.started()

    args = parse_args()

    csv_tables = get_csv_tables(args.csv_dir)

    ingest_data(args.db, csv_tables, args.replace)

    log.finished()


def ingest_data(db_path, csv_tables, replace):
    if_exists = "replace" if replace else "append"
    with sqlite3.connect(db_path) as cxn:
        for table in TABLES:
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
    """Normalize column names to reduce the amount of Field columns to store."""
    renames = {c: c.lower() for c in df.columns}
    df = df.rename(renames, axis="columns")
    return df


def extract_csv_data(csv_tables, db_table):
    all_csv_data = []

    all_targets = [f.columns for f in db_table.fields]

    # Look thru all CSV dataframes
    for table_name, csv_table in csv_tables.items():
        csv_column_set = set(csv_table.columns)

        best_cover = csv_column_set

        # Examine every possible combination of column names for a match
        for targets in product(*all_targets):
            target_set = set(targets)
            
            cover = set(targets) - set(csv_table.columns)
            if len(cover) < len(best_cover):
                best_cover = cover

            # Did we find all table columns in the CSV?
            if csv_column_set >= target_set:

                db_table_data = csv_table.loc[:, targets]

                # Rename CSV columns to what they'll be in the database table
                renames = {t: f.name for t, f in zip(targets, db_table.fields)}
                db_table_data = db_table_data.rename(renames, axis="columns")

                # Try to get the proper data type for each column
                types = {f.name: TYPE[f.type] for f in db_table.fields}
                db_table_data = db_table_data.astype(types, errors="ignore")

                all_csv_data.append(db_table_data)
                logging.info(f"Hit  {db_table.name} & {table_name}")
                break  # Go look for data in the next CSV table
        else:
            logging.info(f"Miss {db_table.name} & {table_name} Best = {best_cover}")

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
