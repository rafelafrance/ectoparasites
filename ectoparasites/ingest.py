#!/usr/bin/env python3

import argparse
import logging
import sqlite3
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pylib import log


@dataclass
class Field:
    field: str
    type: str
    column: str


@dataclass
class Table:
    name: str
    fields: list[Field]


TABLES = [
    Table("taxonomy", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="species", type="text", column="species"),
    ]),
    Table("gps", [
        Field(field="site", type="categorical", column="site"),
        Field(field="location", type="categorical", column="location"),
        Field(field="latitude", type="numeric", column="lat"),
        Field(field="longitude", type="numeric", column="lon"),
        Field(field="altitude", type="numeric", column="ele"),
    ]),
    Table("site", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="site", type="categorical", column="site"),
        Field(field="location", type="categorical", column="location"),
    ]),
    Table("date", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="day", type="numerical", column="day"),
        Field(field="month", type="numerical", column="month"),
        Field(field="year", type="numerical", column="year"),
    ]),
    Table("bird_quantitative", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="mass", type="categorical", column="mass"),
        Field(field="tar", type="numerical", column="tar"),
        Field(field="ct", type="numerical", column="ct"),
        Field(field="ce", type="numerical", column="ce"),
        Field(field="pw", type="numerical", column="pw"),
        Field(field="ph", type="numerical", column="ph"),
        Field(field="com", type="numerical", column="com"),
        Field(field="halux", type="numerical", column="hal"),
        Field(field="nail", type="numerical", column="uña"),
        Field(field="claw_extension", type="numerical", column="ext"),
        Field(field="rc", type="numerical", column="rc"),
        Field(field="re", type="numerical", column="re"),
        Field(field="wing", type="numerical", column="ala"),
        Field(field="p_s", type="numerical", column="p-s"),
    ]),
    Table("bird_categorical", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="age", type="categorical", column="age"),
        # Field(field="how_aged", type="categorical", column="how aged"),
        Field(field="sex", type="categorical", column="sex"),
        # Field(field="how_sexed", type="categorical", column="how sexed"),
        Field(field="reproduction", type="categorical", column="rep"),
        Field(field="fat", type="categorical", column="fat"),
        Field(field="bm", type="categorical", column="bm"),
        Field(field="fm", type="categorical", column="fm"),
        # Field(field="pec_muscle", type="categorical", column="musculo"),
        # Field(field="pro_cloacal", type="categorical", column="pro_cloacal"),
        # Field(field="brood_patch", type="categorical", column="parche_inc"),
        Field(field="recapture", type="y/n", column="recap"),
        # Field(field="status", type="categorical", column="status"),
        # Field(field="leg_color", type="categorical", column="leg color"),
        # Field(field="orbital_color", type="categorical", column="orbital color"),
        # Field(field="skull", type="categorical", column="skull"),
        Field(field="notes", type="categorical", column="notes"),
    ]),
    Table("bird_samples", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        Field(field="blood", type="y/n", column="blood"),
        Field(field="ectos", type="y/n", column="paras"),
        Field(field="feather", type="y/n", column="feather"),
        # Field(field="collect_number", type="numerical", column="Num_Colecta"),
        Field(field="photo", type="y/n", column="photo#"),
    ]),
    Table("bird_band", [
        Field(field="capture_id", type="categorical", column="id"),
        Field(field="band", type="categorical", column="band"),
        # Field(field="ring_color", type="categorical", column="color_anillo"),
        # Field(field="bander", type="text", column="anillador"),
        # Field(field="banding_day", type="categorical", column="banding day"),
    ]),
    # Table("net_location", [
    #     Field(field="capture_id", type="categorical", column="id"),
    #     Field(field="band", type="categorical", column="band"),
    #     Field(field="site", type="categorical", column="site"),
    #     Field(field="location", type="categorical", column="location"),
    #     # Field(field="net_number", type="categorical", column="net"),
    # ]),
    # Table("positive_ectos", [
    #     Field(field="capture_id", type="categorical", column="id"),
    #     Field(field="box", type="", column="box"),
    #     Field(field="box_position", type="", column="box_position"),
    #     Field(field="bird_specie", type="", column="bird_specie"),
    #     Field(field="ecto_group", type="", column="ecto_group"),
    #     Field(field="station", type="", column="station"),
    #     Field(field="capture_id", type="", column="capture_id"),
    #     Field(field="date", type="", column="date"),
    #     Field(field="format_date", type="", column="format_date"),
    #     Field(field="year", type="", column="year"),
    #     Field(field="month", type="", column="month"),
    #     Field(field="day", type="", column="day"),
    #     Field(field="band", type="", column="band"),
    #     Field(field="old_box_position", type="", column="old_box_position"),
    #     Field(field="notes", type="", column="notes"),
    # ]),
    # Table("ectos", [
    #     Field(field="id", type="categorical", column="id"),
    #     Field(field="band", type="categorical", column="band"),
    #     Field(field="ectos", type="y/n", column="ectos"),
    #     Field(field="ecto_type", type="categorical", column="ecto_type"),
    #     Field(field="looked_ectos", type="y/n", column="looked_ectos"),
    #     Field(field="ectos_technique", type="categorical", column="ectos_technique"),
    # ]),
]


def main():
    log.started()

    args = parse_args()

    dfs = {}
    for path in sorted(args.csv_dir.glob("*.csv")):
        df = pd.read_csv(path)
        dfs[path.stem.lower()] = fix_column_names(df)

    with sqlite3.connect(args.db) as cxn:
        for table in TABLES:
            logging.info(f"Getting data for: {table.name}")
            targets = [f.column for f in table.fields]
            renames = {f.column: f.field for f in table.fields}
            df = get_data(dfs, targets)
            df = df.rename(renames, axis="columns")
            df.to_sql(table.name, con=cxn, index=False, if_exists="replace")

    log.finished()


def fix_column_names(df):
    renames = {c: c.lower() for c in df.columns}
    df = df.rename(renames, axis="columns")
    return df


def get_data(dfs, targets):
    target_set = set(targets)
    new = []
    for name, df in dfs.items():
        # print(target_set - set(df.columns))
        if set(df.columns) >= target_set:
            new.append(df.loc[:, targets])
    df = pd.concat(new)
    df = df.drop_duplicates()
    return df


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

    return parser.parse_args()


if __name__ == "__main__":
    main()
