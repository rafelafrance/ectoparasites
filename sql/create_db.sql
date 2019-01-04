DROP TABLE IF EXISTS version;
CREATE TABLE version (
  version TEXT NOT NULL,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id  TEXT NOT NULL PRIMARY KEY,
  description TEXT NOT NULL,
  version     TEXT NOT NULL,
  extracted   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS taxa;
CREATE TABLE taxa (
  sci_name    TEXT  NOT NULL PRIMARY KEY,
  binary      TEXT,
  "order"     TEXT,
  family      TEXT,
  genus       TEXT
);
CREATE INDEX taxa_sci_name ON taxa (sci_name);
CREATE INDEX taxa_binary ON taxa (binary);
CREATE INDEX taxa_order  ON taxa ("order");
CREATE INDEX taxa_family ON taxa (family);
CREATE INDEX taxa_genus  ON taxa (genus);

DROP TABLE IF EXISTS sites;
CREATE TABLE sites (
  site_id            INTEGER NOT NULL PRIMARY KEY,
  dataset_id         TEXT NOT NULL,
  verbatim_long      TEXT,
  verbatim_lat       TEXT,
  verbatim_elevation TEXT,
  long               NUMERIC,
  lat                NUMERIC,
  elevation          NUMERIC,
  country            TEXT,
  road_location      TEXT,
  dataset_site_id    TEXT,
  site_json          TEXT
);
CREATE INDEX sites_dataset_id ON sites (dataset_id);
CREATE INDEX sites_long       ON sites (long);
CREATE INDEX sites_lat        ON sites (lat);
CREATE INDEX sites_elevation  ON sites (elevation);

DROP TABLE IF EXISTS hosts;
CREATE TABLE hosts (
  host_id         INTEGER NOT NULL PRIMARY KEY,
  site_id         INTEGER NOT NULL,
  host_sci_name   TEXT NOT NULL,
  verbatim_sex    TEXT,
  dataset_host_id TEXT,
  host_json       TEXT
);
CREATE INDEX hosts_site_id    ON hosts (site_id);
CREATE INDEX hosts_host_sci_name   ON hosts (host_sci_name);
CREATE INDEX hosts_dataset_host_id ON hosts (dataset_host_id);

DROP TABLE IF EXISTS samples;
CREATE TABLE samples (
  sample_id            INTEGER NOT NULL PRIMARY KEY,
  host_id              INTEGER NOT NULL,
  verbatim_sample_date TEXT,
  verbatim_mass        TEXT,
  verbatim_age         TEXT,
  method               TEXT,
  sample_date          TEXT,
  mass                 NUMERIC,
  ectoparasites        BOOLEAN,
  ectoprocessed        BOOLEAN,
  box_location         TEXT,
  box_id               TEXT,
  vial_no              TEXT,
  dataset_sample_id    TEXT,
  sample_json          TEXT
);
CREATE INDEX samples_sample_id    ON samples (sample_id);
CREATE INDEX samples_host_id      ON samples (host_id);
CREATE INDEX samples_sample_date  ON samples (sample_date);
CREATE INDEX samples_dataset_sample_id ON samples (dataset_sample_id);

DROP TABLE IF EXISTS parasite_groups;
CREATE TABLE parasites (
  parasite_group_id   INTEGER NOT NULL PRIMARY KEY,
  sample_id           INTEGER NOT NULL,
  "group"             TEXT NOT NULL,
  abundance           TEXT,
  locality_tube       TEXT,
  vial_no             TEXT,
  dna_extracted       BOOLEAN,
  dna_method          TEXT,
  pathogen_dna_id     TEXT,
  species_slide       BOOLEAN,
  parasite_group_json TEXT
);
CREATE INDEX parasite_groups_group      ON parasite_groups ("group");
CREATE INDEX parasite_groups_sample_id  ON parasite_groups (sample_id);

DROP TABLE IF EXISTS parasites;
CREATE TABLE parasites (
  parasite_id       INTEGER NOT NULL PRIMARY KEY,
  parasite_group_id INTEGER NOT NULL,
  parasite_sci_name TEXT NOT NULL,
  "count"           INTEGER NOT NULL,
  parasite_json     TEXT
);
CREATE INDEX parasites_parasite_group_id ON parasites (parasite_group_id);
CREATE INDEX parasites_parasite_sci_name ON parasites (parasite_sci_name);
