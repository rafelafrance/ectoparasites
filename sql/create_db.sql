DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id  TEXT NOT NULL PRIMARY KEY,
  description TEXT NOT NULL,
  version     TEXT NOT NULL,
  extracted   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS taxa;
CREATE TABLE taxa (
  taxon_id    INTEGER NOT NULL PRIMARY KEY,
  sci_name    TEXT UNIQUE,
  binary      TEXT,
  "group"     TEXT,
  "order"     TEXT,
  family      TEXT,
  genus       TEXT
);
CREATE INDEX taxa_sci_name ON taxa (sci_name);
CREATE INDEX taxa_binary ON taxa (binary);
CREATE INDEX taxa_group  ON taxa ("group");
CREATE INDEX taxa_order  ON taxa ("order");
CREATE INDEX taxa_family ON taxa (family);
CREATE INDEX taxa_genus  ON taxa (genus);

DROP TABLE IF EXISTS sites;
CREATE TABLE sites (
  site_id          INTEGER NOT NULL PRIMARY KEY,
  dataset_id       TEXT NOT NULL,
  long             NUMERIC,
  lat              NUMERIC,
  radius           NUMERIC,
  elevation        NUMERIC,
  elevation_radius NUMERIC,
  country          TEXT,
  road_location    TEXT,
  dataset_site_id  TEXT,
  geohash          TEXT,
  site_json        TEXT
);
CREATE INDEX sites_dataset_id ON sites (dataset_id);
CREATE INDEX sites_long       ON sites (long);
CREATE INDEX sites_lat        ON sites (lat);
CREATE INDEX sites_elevation  ON sites (elevation);
CREATE INDEX sites_geohash    ON sites (geohash);

DROP TABLE IF EXISTS hosts;
CREATE TABLE hosts (
  host_id         INTEGER NOT NULL PRIMARY KEY,
  site_id         INTEGER NOT NULL,
  host_taxon_id   INTEGER NOT NULL,
  dataset_id      TEXT NOT NULL,
  method          TEXT,
  mass            NUMERIC,
  age             TEXT,
  sex             TEXT,
  dataset_host_id TEXT,
  host_json       TEXT
);
CREATE INDEX hosts_site_id    ON hosts (site_id);
CREATE INDEX hosts_dataset_id ON hosts (dataset_id);
CREATE INDEX hosts_taxon_id   ON hosts (host_taxon_id);
CREATE INDEX hosts_dataset_host_id ON hosts (dataset_host_id);

DROP TABLE IF EXISTS samples;
CREATE TABLE samples (
  sample_id         INTEGER NOT NULL PRIMARY KEY,
  host_id           INTEGER NOT NULL,
  dataset_id        TEXT NOT NULL,
  sample_date       TEXT,
  method            TEXT,
  ectoparasites     BOOLEAN,
  ectoprocessed     BOOLEAN,
  box_location      TEXT,
  box_id            TEXT,
  vial_no           TEXT,
  dataset_sample_id TEXT,
  sample_json       TEXT
);
CREATE INDEX samples_sample_id    ON samples (sample_id);
CREATE INDEX samples_host_id      ON samples (host_id);
CREATE INDEX samples_dataset_id   ON samples (dataset_id);
CREATE INDEX samples_sample_date  ON samples (sample_date);
CREATE INDEX samples_dataset_sample_id ON samples (dataset_sample_id);

DROP TABLE IF EXISTS parasites;
CREATE TABLE parasites (
  parasite_id        INTEGER NOT NULL PRIMARY KEY,
  sample_id          INTEGER NOT NULL,
  parasite_taxon_id  INTEGER NOT NULL,
  dataset_id         TEXT NOT NULL,
  verbatim_abundance TEXT,
  abundance          INTEGER,
  locality_tube      TEXT,
  vial_no            TEXT,
  dna_extracted      BOOLEAN,
  dna_method         TEXT,
  pathogen_dna_id    TEXT,
  species_slide      BOOLEAN,
  parasite_json      TEXT
);
CREATE INDEX parasites_parasite_taxon_id ON parasites (parasite_taxon_id);
CREATE INDEX parasites_sample_id  ON parasites (sample_id);
CREATE INDEX parasites_dataset_id ON parasites (dataset_id);
