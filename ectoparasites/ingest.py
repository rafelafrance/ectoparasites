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
class Column:
    name: str
    type: str
    csv: list[str]


@dataclass
class Table:
    name: str
    columns: list[Column]


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
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(
            name="species",
            type="text",
            csv=["species", "bird species", "specie", "bird_specie"],
        ),
    ]),
    Table("gps", [
        Column(name="site", type="categorical", csv=["site"]),
        Column(name="location", type="categorical", csv=["location", "localidad"]),
        Column(name="latitude", type="numeric", csv=["lat"]),
        Column(name="longitude", type="numeric", csv=["lon"]),
        Column(name="altitude", type="numeric", csv=["ele"]),
    ]),
    Table("site", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="site", type="categorical", csv=["site"]),
        Column(name="location", type="categorical", csv=["location", "localidad"]),
    ]),
    Table("date", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="day", type="int", csv=["day"]),
        Column(name="month", type="text", csv=["month"]),
        Column(name="year", type="int", csv=["year"]),
    ]),
    Table("bird_quantitative", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="mass", type="categorical", csv=["mass"]),
        Column(name="tar", type="numerical", csv=["tar"]),
        Column(name="ct", type="numerical", csv=["ct"]),
        Column(name="ce", type="numerical", csv=["ce"]),
        Column(name="pw", type="numerical", csv=["pw"]),
        Column(name="ph", type="numerical", csv=["ph"]),
        Column(name="com", type="numerical", csv=["com"]),
        Column(name="halux", type="numerical", csv=["hal"]),
        Column(name="nail", type="numerical", csv=["uña", "una"]),
        Column(name="claw_extension", type="numerical", csv=["ext"]),
        Column(name="rc", type="numerical", csv=["rc"]),
        Column(name="re", type="numerical", csv=["re"]),
        Column(name="wing", type="numerical", csv=["ala"]),
        Column(name="p_s", type="numerical", csv=["p-s"]),
    ]),
    Table("bird_categorical", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="age", type="categorical", csv=["age"]),
        # Field(name="how_aged", type="categorical", csv=["how aged"]),
        Column(name="sex", type="categorical", csv=["sex"]),
        # Field(name="how_sexed", type="categorical", csv=["how sexed"]),
        Column(name="reproduction", type="categorical", csv=["rep", "rep."]),
        Column(name="fat", type="categorical", csv=["fat"]),
        Column(name="bm", type="categorical", csv=["bm"]),
        Column(name="fm", type="categorical", csv=["fm"]),
        # Field(name="pec_muscle", type="categorical", csv=["musculo"]),
        # Field(name="pro_cloacal", type="categorical", csv=["pro_cloacal"]),
        # Field(name="brood_patch", type="categorical", csv=["parche_inc"]),
        Column(name="recapture", type="y/n", csv=["recap"]),
        # Field(name="status", type="categorical", csv=["status"]),
        # Field(name="leg_color", type="categorical", csv=["leg color"]),
        # Field(name="orbital_color", type="categorical", csv=["orbital color"]),
        # Field(name="skull", type="categorical", csv=["skull"]),
        Column(name="notes", type="categorical", csv=["notes", "observaciones"]),
    ]),
    Table("bird_samples", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="blood", type="y/n", csv=["blood"]),
        Column(name="ectos", type="y/n", csv=["paras"]),
        Column(name="feather", type="y/n", csv=["feather"]),
        # Field(name="collect_number", type="numerical", csv=["num_colecta"]),
        Column(name="photo", type="y/n", csv=["photo#", "photo"]),
    ]),
    Table("bird_band", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        # Field(name="ring_color", type="categorical", csv=["color_anillo"]),
        # Field(name="bander", type="text", csv=["anillador"]),
        # Field(name="banding_day", type="categorical", csv=["banding day"]),
    ]),
    # Table("net_location", [
    #     Field(name="capture_id", type="categorical", csv=["id"]),
    #     Field(name="band", type="categorical", csv=["band"]),
    #     Field(name="site", type="categorical", csv=["site"]),
    #     Field(name="location", type="categorical", csv=["location"]),
    #     # Field(name="net_number", type="categorical", csv=["net"]),
    # ]),
    # Table("positive_ectos", [
    #     Field(name="capture_id", type="categorical", csv=["id"]),
    #     Field(name="box", type="", csv=["box"]),
    #     Field(name="box_position", type="", csv=["box_position"]),
    #     Field(name="bird_specie", type="", csv=["bird_specie"]),
    #     Field(name="ecto_group", type="", csv=["ecto_group"]),
    #     Field(name="station", type="", csv=["station"]),
    #     Field(name="capture_id", type="", csv=["capture_id"]),
    #     Field(name="date", type="", csv=["date"]),
    #     Field(name="format_date", type="", csv=["format_date"]),
    #     Field(name="year", type="", csv=["year"]),
    #     Field(name="month", type="", csv=["month"]),
    #     Field(name="day", type="", csv=["day"]),
    #     Field(name="band", type="", csv=["band"]),
    #     Field(name="old_box_position", type="", csv=["old_box_position"]),
    #     Field(name="notes", type="", csv=["notes"]),
    # ]),
    # Table("ectos", [
    #     Field(name="id", type="categorical", csv=["id"]),
    #     Field(name="band", type="categorical", csv=["band"]),
    #     Field(name="ectos", type="y/n", csv=["ectos"]),
    #     Field(name="ecto_type", type="categorical", csv=["ecto_type"]),
    #     Field(name="looked_ectos", type="y/n", csv=["looked_ectos"]),
    #   Field(name="ectos_technique", type="categorical", csv=["ectos_technique"]),
    # ]),
    # Table("dataset", [
    #     Field(name="dataset_id", type="text", csv=[]),  Auto generate
    #     Field(name="principle_investigator", type="categorical", csv=[]), Literal
    #     Field(name="data_type", type="categorical", csv=[]), Literal
    #     Field(name="country", type="categorical", csv=[]), Literal
    # ]),
    # Table("site", [
    # ]),
    # Table("locality", [
    #     Field(name="site_id", type="text", csv=["site_id"]),
    #     Field(name="location_id", type="categorical", csv=["location", "localidad"]),
    #     Field(name="latitude", type="numeric", csv=["lat"]),
    #     Field(name="longitude", type="numeric", csv=["lon"]),
    #     Field(name="elevation", type="numeric", csv=["ele"]),
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

    all_targets = [f.csv for f in db_table.columns]

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
                renames = {t: f.name for t, f in zip(targets, db_table.columns)}
                db_table_data = db_table_data.rename(renames, axis="columns")

                # Try to get the proper data type for each column
                types = {f.name: TYPE[f.type] for f in db_table.columns}
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
