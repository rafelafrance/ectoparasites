# ectoparasites
Utilities and database supporting ectoparasite research.

## Install
You will need GIT to clone this repository. You will also need to have Python3.11+ installed, as well as pip, a package manager for Python.
You can install the requirements into your python environment like so:
```bash
git clone https://github.com/rafelafrance/ectoparasites
cd ectoparasites
python3 -m pip install .
```
I do recommend using a virtual environment but that is not required.

## ingest.py

This is a script that ingests cleaned CSV data into a database.
The database is SQLite for now but we can modify it to work with other database.
The script is data-driven with most of the logic stored in a big `TABLES` global list of table objects.
The script has a relatively small amount of logic to process this data.

**Note that each CSV file may contain data for several database tables and each database table may have data in several CSV files.**
It's a many-to-many situation where a CSV can match many database tables and a database table can match many CSVs. 

Each Table has a:
1. name: The database table name.
2. fields: Is a list of field classes.

Each Field contains:
1. name: The database column name.
2. type: The data type for the database column.
3. columns: A list of potential column names in the CSV that map to this database columns.
   1. For example the "species" database column will map to "species" in some CSVs and to "bird_specie" in other CSVs.

Program logic:
1. `./ectoparasites/ingest.py --db /path/to/your/database.sqlite --csv-dir /path/to/raw/csv/data/dir` 
   1. Use the `--replace` option to replace data in the tables and leave it out to append data.
2. The program scan the given `--csv-dir` for all CSVs in it.
3. For every `Table` object in the `TABLES` list it looks in each CSV file to see if all the database table's columns are contained in it. If all database columns are in the CSV file:
      1. We extract the subset of columns from the CSV file for the DB table.
      2. Rename the CSV columns to match the database columns.
      3. Update the data type to match what we need for the database column.
      4. Write the data to the database table.
