from dataclasses import dataclass
from itertools import product


@dataclass
class Column:
    name: str
    type: str
    csv: list[str]  # If a column is optional add a None entry in this list


@dataclass
class Table:
    name: str
    columns: list[Column]

    def csv_permutations(self):
        options = [c.csv for c in self.columns]
        col_sets = [[c for c in p if c] for p in product(*options)]
        # We want as many columns as possible so try the longer lists first
        col_sets = sorted(col_sets, key=lambda s: len(s), reverse=True)
        return col_sets


SQLITE_TYPE = {
    "categorical": "string",
    "int": "int32",
    "numeric": "float32",
    "numerical": "float32",
    "date": "string",
    "text":  "string",
    "time":  "string",
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
        Column(name="how_aged", type="categorical", csv=["how aged"]),
        Column(name="sex", type="categorical", csv=["sex"]),
        Column(name="how_sexed", type="categorical", csv=["how sexed"]),
        Column(name="reproduction", type="categorical", csv=["rep", "rep."]),
        Column(name="fat", type="categorical", csv=["fat"]),
        Column(name="bm", type="categorical", csv=["bm"]),
        Column(name="fm", type="categorical", csv=["fm"]),
        Column(name="pec_muscle", type="categorical", csv=["musculo"]),
        Column(name="pro_cloacal", type="categorical", csv=["pro_cloacal"]),
        Column(name="brood_patch", type="categorical", csv=["parche_inc"]),
        Column(name="recapture", type="y/n", csv=["recap"]),
        Column(name="status", type="categorical", csv=["status"]),
        Column(name="leg_color", type="categorical", csv=["leg color"]),
        Column(name="orbital_color", type="categorical", csv=["orbital color"]),
        Column(name="skull", type="categorical", csv=["skull"]),
        Column(name="notes", type="categorical", csv=["notes", "observaciones"]),
    ]),
    Table("bird_samples", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="blood", type="y/n", csv=["blood"]),
        Column(name="ectos", type="y/n", csv=["paras"]),
        Column(name="feather", type="y/n", csv=["feather"]),
        Column(name="collect_number", type="numerical", csv=["num_colecta"]),
        Column(name="photo", type="y/n", csv=["photo#", "photo"]),
    ]),
    Table("bird_band", [
        Column(name="capture_id", type="categorical", csv=["id"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="ring_color", type="categorical", csv=["color_anillo"]),
        Column(name="bander", type="text", csv=["anillador"]),
        Column(name="banding_day", type="categorical", csv=["banding day"]),
    ]),
    Table("net_location", [
       Column(name="capture_id", type="categorical", csv=["id"]),
       Column(name="band", type="categorical", csv=["band"]),
       Column(name="site", type="categorical", csv=["site"]),
       Column(name="location", type="categorical", csv=["location"]),
       Column(name="net_number", type="categorical", csv=["net"]),
    ]),
    Table("positive_ectos", [
       Column(name="capture_id", type="categorical", csv=["id"]),
       Column(name="box", type="", csv=["box"]),
       Column(name="box_position", type="", csv=["box_position"]),
       Column(name="bird_specie", type="", csv=["bird_specie"]),
       Column(name="ecto_group", type="", csv=["ecto_group"]),
       Column(name="station", type="", csv=["station"]),
       Column(name="capture_id", type="", csv=["capture_id"]),
       Column(name="date", type="", csv=["date"]),
       Column(name="format_date", type="", csv=["format_date"]),
       Column(name="year", type="", csv=["year"]),
       Column(name="month", type="", csv=["month"]),
       Column(name="day", type="", csv=["day"]),
       Column(name="band", type="", csv=["band"]),
       Column(name="old_box_position", type="", csv=["old_box_position"]),
       Column(name="notes", type="", csv=["notes"]),
    ]),
    Table("ectos", [
       Column(name="id", type="categorical", csv=["id"]),
       Column(name="band", type="categorical", csv=["band"]),
       Column(name="ectos", type="y/n", csv=["ectos"]),
       Column(name="ecto_type", type="categorical", csv=["ecto_type"]),
       Column(name="looked_ectos", type="y/n", csv=["looked_ectos"]),
     Column(name="ectos_technique", type="categorical", csv=["ectos_technique"]),
    ]),
    Table("dataset", [
       Column(name="dataset_id", type="text", csv=[None]),  # Auto generate?
       Column(name="principle_investigator", type="categorical", csv=[None]), # Literal?
       Column(name="data_type", type="categorical", csv=[None]),  # Literal?
       Column(name="country", type="categorical", csv=[None]),  # Literal?
    ]),
    Table("site", [
       Column(name="site_id", type="text", csv=[None]),
       Column(name="name", type="text", csv=["station"]),
       Column(name="country", type="text", csv=[None]),
       Column(name="municipality", type="text", csv=[None]),
       Column(name="upper_latitude", type="numeric", csv=[None]),
       Column(name="lower_latitude", type="numeric", csv=[None]),
       Column(name="central_point_latitude", type="numeric", csv=[None]),
       Column(name="central_point_longitude", type="numeric", csv=[None]),
       Column(name="central_elevation", type="numeric", csv=[None]),
       Column(name="upper_elevation", type="numeric", csv=[None]),
       Column(name="lower_elevation", type="numeric", csv=[None]),
       Column(name="habitat", type="text", csv=[None]),
       Column(name="notes", type="text", csv=[None]),
    ]),
    Table("locality_2", [
       Column(name="location_id", type="categorical", csv=["location", "localidad"]),
       Column(name="site_id", type="text", csv=["site_id"]),
       Column(name="latitude", type="numeric", csv=["lat"]),
       Column(name="longitude", type="numeric", csv=["lon"]),
       Column(name="elevation", type="numeric", csv=["ele"]),
       Column(name="elevation_source", type="text", csv=[None]),
    ]),
    Table("point", [
        Column(name="point_id", type="text", csv=["point_id"]),
        Column(name="point_name", type="text", csv=["point", None]),
        Column(name="location_id", type="text", csv=["location_id"]),
        Column(name="site_id", type="text", csv=["site_id", "site"]),
        Column(name="net_number", type="numeric", csv=["net", None]),
        Column(name="latitude", type="numeric", csv=[
            "decimallatitude", "point_latitude", "lat"
        ]),
        Column(name="longitude", type="numeric", csv=[
            "decimallongitude", "point_longitude", "long"
        ]),
        Column(name="elevation", type="numeric", csv=[
            "elevation_meters", "net_elevation", "elevation"
        ]),
        Column(
            name="elevation_source",
            type="text",
            csv=["elevation_source", None]
        ),
    ]),
    Table("sample", [
        Column(name="sample_id", type="text", csv=["sample_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="site_id", type="text", csv=["site_id"]),
        Column(name="location_id", type="text", csv=["location_id"]),
        Column(name="point_id", type="text", csv=["point_id"]),
        Column(name="data_type", type="text", csv=["data_type"]),
        Column(name="date_", type="text", csv=["day/month/year"]),
        Column(name="genus_species", type="text", csv=["species"]),
        Column(name="family", type="text", csv=["family", None]),
        Column(name="taxon_id", type="text", csv=[None]),
    ]),
    Table("taxonomy", [
        Column(name="taxon_id", type="text", csv=[None]),
        Column(name="genus_species", type="text", csv=[None]),
        Column(name="genus", type="text", csv=[None]),
        Column(name="species", type="text", csv=[None]),
        Column(name="species_sacc_2021", type="text", csv=[None]),
        Column(name="species_clements_2022", type="text", csv=[None]),
        Column(name="family", type="text", csv=[None]),
        Column(name="order_", type="text", csv=[None]),
        Column(name="synonyms", type="text", csv=[None]),
    ]),
    Table("collections", [
        Column(name="sample_id", type="text", csv=["sample_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="data_type", type="text", csv=["data_type"]),
        Column(name="feathers", type="y/n", csv=["blood", None]),  # ?!
        Column(name="blood", type="y/n", csv=["ecto", None]),  # ?!
        Column(name="ectos", type="y/n", csv=["feathers", None]),  # ?!
        Column(name="metabolics", type="y/n", csv=["metabolics", None]),
        Column(name="pollen", type="y/n", csv=["pollen", None]),
        Column(name="smear", type="y/n", csv=["smear", None]),
        Column(name="fecal", type="y/n", csv=["fecal", None]),
        Column(name="photo", type="y/n", csv=["photo", None]),
        Column(name="recording", type="y/n", csv=["recording", None]),
    ]),
    Table("raw_data_info", [
        Column(name="sample_id", type="text", csv=["sample_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="data_type", type="text", csv=["data_type"]),
        Column(
            name="orange_notebook_initials",
            type="text",
            csv=["orange_notebook_initials"],
        ),
        Column(
            name="orange_notebook_page",
            type="text",
            csv=["orange_notebook_page", None]
        ),
        Column(
            name="orange_notebook_page",
            type="text",
            csv=["orange_notebook_page", None]
        ),
        Column(
            name="orange_notebook_name",
            type="text",
            csv=["orange_notebook_name", None]
        ),
        Column(
            name="orange_notebook_number",
            type="text",
            csv=["orange_notebook_number", None]
        ),
        Column(
            name="nest_searcher_id",
            type="text",
            csv=["nest_searcher_id", None]
        ),
        Column(
            name="nest_searcher_initials",
            type="text",
            csv=["nest_searcher_initials", None]
        ),
        Column(name="field_id", type="text", csv=["field_id", None]),
        Column(name="bander", type="text", csv=["bander", None]),
        Column(name="observer", type="text", csv=["observer", None]),
        Column(name="capture_time", type="time", csv=["capture_time", None]),
        Column(name="tube_label", type="text", csv=["tube_label", None]),
        Column(
            name="spreadsheet_id",
            type="text",
            csv=["id_db_captures", "id_db_pointcounts", None]
        ),
    ]),
    Table("nest", [
        Column(name="nest_id", type="text", csv=["unique_id_nest"]),
        Column(name="taxonomy_id", type="text", csv=["taxonomy_id"]),
        Column(name="site_id", type="text", csv=["site_id"]),
        Column(name="location_id", type="text", csv=["location_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="genus", type="text", csv=["species"]),  # ?!
        Column(name="species", type="text", csv=["species"]),  # ?!
        Column(name="genus_species", type="text", csv=["species"]),  # ?!
        Column(name="corrected_id", type="text", csv=["correct_id_nest"]),
        Column(name="year", type="date", csv=["season_year"]),
        Column(name="date_nest_found", type="date", csv=[None]),
        Column(name="date_start_laying", type="date", csv=[None]),
        Column(name="date_end_laying", type="date", csv=[None]),
        Column(name="date_hatching", type="date", csv=[None]),
        Column(name="date_nestling_found", type="date", csv=[None]),
        Column(name="date_last_active", type="date", csv=[None]),
        Column(name="date_last_checked", type="date", csv=[None]),
    ]),
    Table("nest_eggs", [
        Column(name="egg_id", type="text", csv=["egg_id"]),
        Column(name="nest_id", type="text", csv=["unique_id_nest"]),
        Column(name="genus_species", type="text", csv=["species"]),
        Column(name="taxonomy_id", type="text", csv=["taxonomy_id"]),
        Column(name="genus", type="text", csv=["species"]),  # ?!
        Column(name="species", type="text", csv=["species"]),  # ?!
        Column(
            name="embryonic_development",
            type="text",
            csv=["egg_embryonic_development"],
        ),
        Column(
            name="embryonic_development_mass_based",
            type="text",
            csv=["egg_embryonic_development_mass_based"],
        ),
        Column(name="length", type="numeric", csv=["egg_length_mm"]),
        Column(name="width", type="numeric", csv=["egg_width_mm"]),
        Column(name="weight", type="numeric", csv=["egg_weight_gr"]),
        Column(name="clutch_size", type="numeric", csv=["clutch_size"]),
        Column(name="weight_measurement_order", type="text", csv=["measure_weight"]),
    ]),
    Table("nest_morphology", [
        Column(name="nest_id", type="text", csv=["unique_id_nest"]),
        Column(name="internal_length", type="numeric", csv=["nest_internal_length_mm"]),
        Column(name="internal_width", type="numeric", csv=["nest_internal_width_mm"]),
        Column(name="wall_thickness", type="numeric", csv=["nest_wall_thickness_mm"]),
        Column(name="external_length", type="numeric", csv=["nest_external_length_mm"]),
        Column(name="external_width", type="numeric", csv=["nest_external_width_mm"]),
        Column(name="external_height", type="numeric", csv=["nest_external_hight_mm"]),
        Column(name="depth_one", type="numeric", csv=["nest_depth_one_mm"]),
        Column(name="depth_two", type="numeric", csv=["nest_depth_two_mm"]),
        Column(
            name="height_above_ground", type="numeric", csv=["nest_height_above_ground"]
        ),
        Column(name="weight", type="numeric", csv=["nest_weight_total"]),
        Column(name="weight_layer_one", type="numeric", csv=["nest_layer_one_weight"]),
        Column(name="weight_layer_two", type="numeric", csv=["nest_layer_two_weigth"]),
        Column(
            name="weight_layer_three", type="numeric", csv=["nest_layer_three_weight"]
        ),
        Column(
            name="weight_layer_four", type="numeric", csv=["nest_layer_four_weight"]
        ),
        Column(name="placement", type="text", csv=["nest_placement"]),
        Column(name="architecture", type="text", csv=["nest_architecture"]),
        Column(name="notes", type="text", csv=["notes"]),
        Column(name="extra_material", type="y/n", csv=["nest_extra_material"]),
        Column(
            name="extra_material_notes", type="text", csv=["nest_extra_material_notes"]
        ),
        Column(name="to_check_shape", type="categorical", csv=["to_check_shape"]),
    ]),
    Table("capture", [
        Column(name="capture_id", type="text", csv=["id_db_captures"]),
        Column(name="sample_id", type="text", csv=["sample_id"]),
        Column(name="site_id", type="text", csv=["site_id", "site"]),
        Column(name="location_id", type="text", csv=["location_id"]),
        Column(name="point_id", type="text", csv=["point_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="recapture", type="y/n", csv=["code"]),
        Column(name="num_recapture", type="categorical", csv=["code"]),
        Column(name="band", type="categorical", csv=["band"]),
        Column(name="date_", type="categorical", csv=["day/month/year"]),
        Column(name="age", type="text", csv=["age"]),
        Column(name="sex", type="text", csv=["sex"]),
        Column(name="capture_time", type="time", csv=["cap_time"]),
        Column(name="taxonomy_id", type="text", csv=["taxonomy_id"]),
        Column(name="genus_species", type="text", csv=["species"]),
        Column(name="dead", type="text", csv=["dead"]),
    ]),
    Table("bird_morphology", [
        Column(name="capture_id", type="text", csv=["id_db_captures"]),
        Column(name="age", type="text", csv=["age"]),
        Column(name="age_method_1", type="text", csv=["how_aged_1"]),
        Column(name="age_method_2", type="text", csv=["how_aged_2"]),
        Column(name="sex", type="text", csv=["sex"]),
        Column(name="sex_method_1", type="text", csv=["how_sexed_1"]),
        Column(name="sex_method_2", type="text", csv=["how_sexed_2"]),
        Column(name="skull", type="text", csv=["skull"]),
        Column(name="fat", type="text", csv=["fat"]),
        Column(name="bp", type="text", csv=["bp"]),
        Column(name="cp", type="text", csv=["cp"]),
        Column(name="b_molt", type="text", csv=["b_molt"]),
        Column(name="t_molt", type="text", csv=["t_molt"]),
        Column(name="ff_molt", type="text", csv=["ff_molt"]),
        Column(name="ff_wear", type="text", csv=["ff_wear"]),
        Column(name="wing", type="text", csv=["wing"]),
        Column(name="tail", type="text", csv=["tail"]),
        Column(name="bill_length", type="text", csv=["bill_length"]),
        Column(name="bill_width", type="text", csv=["bill_width"]),
        Column(name="mass", type="text", csv=["mass"]),
        Column(name="striations", type="text", csv=["striations"]),
        Column(name="status", type="text", csv=["status"]),
        Column(name="notes_metadata", type="text", csv=["notes_metadata"]),
        Column(name="station_metadata", type="text", csv=["station_metadata"]),
        Column(name="notes", type="text", csv=["notes"]),
        Column(name="action_taxonomy", type="text", csv=["action_taxonomy"]),
        Column(name="notes_taxonomy", type="text", csv=["notes_taxonomy"]),
    ]),
    Table("point_count", [
        Column(name="point_count_id", type="text", csv=["id_db_pointcounts"]),
        Column(name="sample_id", type="text", csv=["sample_id"]),
        Column(name="site_id", type="text", csv=["site_id"]),
        Column(name="location_id", type="text", csv=["location_id"]),
        Column(name="point_id", type="text", csv=["point_id"]),
        Column(name="dataset_id", type="text", csv=["dataset_id"]),
        Column(name="taxonomy_id", type="text", csv=["taxonomy_id"]),
        Column(name="observer", type="text", csv=["observer"]),
        Column(name="date_", type="date", csv=["day/month/year"]),
        Column(name="point_lat", type="numeric", csv=["lat"]),
        Column(name="point_lon", type="numeric", csv=["long"]),
        Column(name="point_elevation", type="numeric", csv=["elevation"]),
        Column(name="point_time", type="time", csv=["time"]),
        Column(name="species", type="text", csv=["species"]),
        Column(name="method", type="text", csv=["method"]),
        Column(name="tfd", type="text", csv=["tfd"]),
        Column(name="min_0_3", type="text", csv=["min_0_3"]),
        Column(name="min_5_5", type="text", csv=["min_5_5"]),
        Column(name="min_5_8", type="text", csv=["min_5_8"]),
        Column(name="min_8_10", type="text", csv=["min_8_10"]),
        Column(name="snapshot", type="text", csv=["snapshot"]),
        Column(name="other_distance", type="text", csv=["other_distance"]),
        Column(name="flock", type="text", csv=["flock"]),
        Column(name="flock_size", type="text", csv=["flock_size"]),
        Column(name="weather_conditions", type="text", csv=["conditions"]),
        Column(name="cloud_cover", type="text", csv=["cloud_cover"]),
        Column(name="temperature", type="text", csv=["temperature"]),
        Column(name="humidity", type="text", csv=["humidity"]),
        Column(name="wind", type="text", csv=["wind"]),
        Column(name="canopy_height", type="text", csv=["canopy_height"]),
        Column(name="after", type="text", csv=["after"]),
        Column(name="from_rec", type="text", csv=[None]),
        Column(name="f_t", type="text", csv=["f/t"]),
        Column(name="review", type="text", csv=["review"]),
    ]),
]

