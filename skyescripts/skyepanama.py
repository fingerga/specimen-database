import sqlite3
import pandas as pd
import os


event_df = pd.read_csv("Panama2021-Main-Dataset\\Panama2021-EventData.csv")
specimen_df = pd.read_csv("Panama2021-Main-Dataset\\Panama2021-SpecimenData.csv")
dna_df = pd.read_csv("Panama2021-Main-Dataset\\Panama2021-DNAextractions.csv")
library_df = pd.read_csv("Panama2021-Main-Dataset\\Panama2021-GenomicLibraries.csv")

print(f"\nEvent Data:        {event_df.shape[0]} rows, {event_df.shape[1]} columns")
print(f"Specimen Data:     {specimen_df.shape[0]} rows, {specimen_df.shape[1]} columns")
print(f"DNA Extractions:   {dna_df.shape[0]} rows, {dna_df.shape[1]} columns")
print(f"Genomic Libraries: {library_df.shape[0]} rows, {library_df.shape[1]} columns")

print("\n--- Event Data column names ---")
print(list(event_df.columns))

print("\n--- Specimen Data column names ---")
print(list(specimen_df.columns))

print("\n--- DNA Extraction column names ---")
print(list(dna_df.columns))

print("\n--- Genomic Library column names ---")
print(list(library_df.columns))

#################################################
### DATA CLEANING
#################################################

# Renaming columns to be more consistent and descriptive
# rename() takes a dictionary mapping old names -> new names.

event_clean = event_df.rename(
    columns={
        "trip_id": "trip_id",
        "event_code": "event_code",  # PRIMARY KEY
        "fieldwork_status": "fieldwork_status",
        "year": "year",
        "month": "month",
        "day": "day",
        "date": "date",
        "waypoint": "waypoint",
        "latitude": "latitude",
        "longitude": "longitude",
        "environment": "environment",
        "low_tide": "low_tide",
        "collecting_method": "collecting_method",
        "depth": "depth",
        "population": "population",
        "locality": "locality",
        "locality_details": "locality_details",
        "city_district": "city_district",
        "province": "province",
        "area": "area",
        "photos_env": "photos_env",
        "country": "country",
        "collector": "collector",
        "notes": "event_notes",  # Renamed
        "use_in_map": "use_in_map",
    }
)

# remove use_in_map column and population column
# The ~ means "not" — so we keep all columns that do NOT start with "Unnamed"
event_clean = event_clean.loc[:, ~event_clean.columns.str.startswith("use_in_map")]
event_clean = event_clean.loc[:, ~event_clean.columns.str.startswith("population")]

specimen_clean = specimen_df.rename(
    columns={
        "lot_id": "lot_id",  # PRIMARY KEY
        "sufix": "suffix",  # Spelling
        "event_code": "event_code",  # FOREIGN KEY (EventData)
        "species": "species",
        "genus": "genus",
        "epithet": "epithet",
        "clade": "clade",
        "family": "family",
        "development": "development",
        "habitat": "habitat",
        "fixation_method": "fixation_method",
        "specimens": "specimen_count",  # More descriptive
        "parts": "parts",
        "vial": "vial",
        "operculum": "operculum",
        "notes": "specimen_notes",  # Renamed
        "photos_org": "photos_org",
        "identification_by": "identification_by",
        "Voucher": "voucher",  # lowercase
        "SecondVoucherClip": "second_voucher_clip",
    }
)

# Remove unnamed last column
# keep all columns that do NOT start with "Unnamed"
specimen_clean = specimen_clean.loc[
    :, ~specimen_clean.columns.str.startswith("Unnamed")
]

# Need to remove special characters
dna_clean = dna_df.rename(
    columns={
        "extraction_id": "extraction_id",  # PRIMARY KEY
        "lot_id": "lot_id",  # FOREIGN KEY (SpecimenData)
        "species": "species",
        "plate_id": "plate_id",
        "plate_well": "plate_well",
        "extraction_date": "extraction_date",
        "extraction_kit": "extraction_kit",
        "elution_ul": "elution_ul",
        "Qubit_DNA_[ng/ul]": "qubit_dna_ng_ul",  # Removed brackets and slashes
        "Nanodrop_[ng/ul]": "nanodrop_ng_ul",
        "Nanodrop_260/280": "nanodrop_260_280",
        "Nanodrop_260/230": "nanodrop_260_230",
        "Qubit : Nanodrop": "qubit_nanodrop_ratio",
        "clip_over": "clip_over",
        "contamination_plate": "contamination_plate",
        "contamination_wells": "contamination_wells",
        "extraction_notes": "extraction_notes",
        "piece_size": "piece_size",
        "Qubit_after_SpeedVac": "qubit_after_speedvac",
    }
)

library_clean = library_df.rename(
    columns={
        "library_id": "library_id",  # PRIMARY KEY
        "extraction_id": "extraction_id",  # FOREIGN KEY(DNAExtractions)
        "lot_id": "lot_id",
        "species": "species",
        "library_date": "library_date",
        "library_kit": "library_kit",
        "Qubit_DNA_[ng/ul]": "qubit_dna_ng_ul",  # Removed brackets and slashes
        "input_mass_ng": "input_mass_ng",
        "input_vol_ul": "input_vol_ul",
        "EB_complement_ul": "eb_complement_ul",
        "frag_time_min": "frag_time_min",
        "cycles": "cycles",
        "elution_ul": "elution_ul",
        "Qubit_lib_[ng/ul]": "qubit_lib_ng_ul",
        "size_selection": "size_selection",
        "Qubit_size-selection_[ng/ul]": "qubit_size_selection_ng_ul",
        "IDT xGen UDI Primer Pair Well": "idt_primer_well",
        "Primer Name": "primer_name",
        "i5 index": "i5_index",
        "i7 index": "i7_index",
        "Bioanalyzer_avg_size": "bioanalyzer_avg_size",
        "insert_size": "insert_size",
        "[C]_nM_estimate": "concentration_nm_estimate",
        "Data_Target_GB": "data_target_gb",
    }
)

print(f"\nCleaned Event columns:    {list(event_clean.columns)}")
print(f"\nCleaned Specimen columns: {list(specimen_clean.columns)}")
print(f"\nCleaned DNA columns: {list(dna_clean.columns)}")
print(f"\nCleaned Genomic Library columns: {list(library_clean.columns)}")


#################################################
### CREATING SQLITE DATABASE
#################################################

# sqlite3.connect() creates a new .db file (or opens it if it exists)
# The "connection" (conn) is your link to the database file
# The "cursor" (cur) is the object you use to send SQL commands

db_path = "skyescripts\\panama_specimens.db"

# Cleaning outputs made during testing
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Removed existing database at {db_path}")

conn = sqlite3.connect(db_path)
cur = conn.cursor()
print(f"Database created at: {db_path}")

# SQLite has foreign key enforcement off by default
cur.execute("PRAGMA foreign_keys = ON;")

# CREATE TABLE: EventData
cur.execute(
    """
CREATE TABLE IF NOT EXISTS EventData (
    event_code       TEXT PRIMARY KEY,  -- Unique ID for each collection event
    trip_id          TEXT,
    fieldwork_status TEXT,
    year             INTEGER,
    month            INTEGER,
    day              INTEGER,
    date             TEXT,
    waypoint         TEXT,
    latitude         REAL,
    longitude        REAL,
    environment      TEXT,
    low_tide         REAL,
    collecting_method TEXT,
    depth            TEXT,
    population       TEXT,
    locality         TEXT,
    locality_details TEXT,
    city_district    TEXT,
    province         TEXT,
    area             TEXT,
    photos_env       TEXT,
    country          TEXT,
    collector        TEXT,
    event_notes      TEXT,
    use_in_map       TEXT
);
"""
)

# CREATE TABLE: SpecimenData
cur.execute(
    """
CREATE TABLE IF NOT EXISTS SpecimenData (
    lot_id           TEXT PRIMARY KEY,  -- Unique ID for each specimen lot
    suffix           TEXT,
    event_code       TEXT,              -- Links to EventData
    species          TEXT,
    genus            TEXT,
    epithet          TEXT,
    clade            TEXT,
    family           TEXT,
    development      TEXT,
    habitat          TEXT,
    fixation_method  TEXT,
    specimen_count   INTEGER,
    parts            TEXT,
    vial             TEXT,
    operculum        TEXT,
    specimen_notes   TEXT,
    photos_org       TEXT,
    identification_by TEXT,
    voucher          TEXT,
    second_voucher_clip TEXT,
    FOREIGN KEY (event_code) REFERENCES EventData(event_code)
);
"""
)

# CREATE TABLE: DNAExtractions
cur.execute(
    """
CREATE TABLE IF NOT EXISTS DNAExtractions (
    extraction_id         TEXT PRIMARY KEY,  -- Unique ID for each extraction
    lot_id                TEXT,              -- Links to SpecimenData
    species               TEXT,
    plate_id              TEXT,
    plate_well            TEXT,
    extraction_date       TEXT,
    extraction_kit        TEXT,
    elution_ul            REAL,
    qubit_dna_ng_ul       REAL,
    nanodrop_ng_ul        REAL,
    nanodrop_260_280      REAL,
    nanodrop_260_230      REAL,
    qubit_nanodrop_ratio  REAL,
    clip_over             TEXT,
    contamination_plate   TEXT,
    contamination_wells   TEXT,
    extraction_notes      TEXT,
    piece_size            TEXT,
    qubit_after_speedvac  REAL,
    FOREIGN KEY (lot_id) REFERENCES SpecimenData(lot_id)
);
"""
)

# CREATE TABLE: GenomicLibraries
cur.execute(
    """
CREATE TABLE IF NOT EXISTS GenomicLibraries (
    library_id                 TEXT PRIMARY KEY,
    extraction_id              TEXT,           -- Links to DNAExtractions
    lot_id                     TEXT,
    species                    TEXT,
    library_date               TEXT,
    library_kit                TEXT,
    qubit_dna_ng_ul            REAL,
    input_mass_ng              REAL,
    input_vol_ul               REAL,
    eb_complement_ul           REAL,
    frag_time_min              INTEGER,
    cycles                     INTEGER,
    elution_ul                 REAL,
    qubit_lib_ng_ul            REAL,
    size_selection             TEXT,
    qubit_size_selection_ng_ul REAL,
    idt_primer_well            TEXT,
    primer_name                TEXT,
    i5_index                   TEXT,
    i7_index                   TEXT,
    bioanalyzer_avg_size       INTEGER,
    insert_size                INTEGER,
    concentration_nm_estimate  REAL,
    data_target_gb             REAL,
    FOREIGN KEY (extraction_id) REFERENCES DNAExtractions(extraction_id)
);
"""
)

conn.commit()
print("All four tables created successfully.")

#################################################
### LOADING DATA INTO DATABASE
#################################################

"""
pandas - to_sql() writes a DataFrame directly to a SQL table

Parameters:
    name: the name of the table in the database
    con:the database connection
    if_exists: 'append' adds to existing table; 'replace' would overwrite it
    index: False means don't write the pandas row index as a column
"""

event_clean.to_sql("EventData", conn, if_exists="append", index=False)
print(f"Loaded {len(event_clean)} rows into EventData")

# If a specimen references an event code that doesnt exist yet, it causes an error when loading
# Remove those rows and deal with them later

valid_event_codes = set(event_clean["event_code"])
orphan_specimens = specimen_clean[~specimen_clean["event_code"].isin(valid_event_codes)]
valid_specimens = specimen_clean[specimen_clean["event_code"].isin(valid_event_codes)]

if len(orphan_specimens) > 0:
    print(
        f"\n  NOTE: {len(orphan_specimens)} specimen row(s) reference unknown event codes "
        f"and were skipped: {orphan_specimens['event_code'].unique().tolist()}"
    )
    orphan_specimens.to_csv("skyescripts\\orphan_specimens.csv", index=False)

valid_specimens.to_sql("SpecimenData", conn, if_exists="append", index=False)
print(f"Loaded {len(valid_specimens)} rows into SpecimenData")

dna_clean.to_sql("DNAExtractions", conn, if_exists="append", index=False)
print(f"Loaded {len(dna_clean)} rows into DNAExtractions")

library_clean.to_sql("GenomicLibraries", conn, if_exists="append", index=False)
print(f"Loaded {len(library_clean)} rows into GenomicLibraries")

conn.commit()
print("\nAll data loaded successfully!")

#################################################
### EXAMPLE QUERIES
#################################################


"""
SQL query structure:

SELECT [columns you want]
FROM   [table name]
WHERE  [filter condition]      <- optional
JOIN   [other table] ON [...]  <- optional, links tables together
ORDER BY [column] ASC/DESC     <- optional, sorts results
LIMIT  [number]                <- optional, caps number of results

The asterisk (*) in SELECT means "all columns"
"""
# ---- Query 1: List all collection events with locations and dates ----

cur.execute(
    """
    SELECT event_code, date, locality, latitude, longitude, environment
    FROM EventData
    ORDER BY date ASC;
"""
)
rows = cur.fetchall()
for row in rows[:5]:  # Print first 5
    print(row)
print(f"  ... ({len(rows)} total events)")


# ---- Query 2: Find all specimens of a specific species ----
cur.execute(
    """
    SELECT lot_id, species, event_code, voucher
    FROM SpecimenData
    WHERE species = 'Fissurella virescens';
"""
)
rows = cur.fetchall()
for row in rows:
    print(row)
print(f"  ({len(rows)} specimens found)")


# ---- Query 3: Find specimens from a specific family ----
print("\n--- Query 3: All specimens from family Fissurellidae ---")
cur.execute(
    """
    SELECT lot_id, species, genus, event_code
    FROM SpecimenData
    WHERE family = 'Fissurellidae'
    ORDER BY genus;
"""
)
rows = cur.fetchall()
for row in rows[:5]:
    print(row)
print(f"  ... ({len(rows)} total Fissurellidae specimens)")


# ---- Query 4: JOIN two tables (combine specimen and event information)
cur.execute(
    """
    SELECT s.lot_id, s.species, e.locality, e.latitude, e.longitude, e.date
    FROM SpecimenData AS s
    JOIN EventData AS e ON s.event_code = e.event_code
    LIMIT 5;
"""
)
rows = cur.fetchall()
for row in rows:
    print(row)


# ---- Query 5: Specimens with associated DNA extractions ----
cur.execute(
    """
    SELECT d.extraction_id, d.lot_id, d.species, d.extraction_date, d.qubit_dna_ng_ul
    FROM DNAExtractions AS d
    ORDER BY d.extraction_date
    LIMIT 5;
"""
)
rows = cur.fetchall()
for row in rows:
    print(row)
total = cur.execute("SELECT COUNT(*) FROM DNAExtractions").fetchone()[0]
print(f"  ({total} total extractions in database)")


# ---- Query 6: JOIN specimen, event, and extraction (three tables)----
cur.execute(
    """
    SELECT 
        s.lot_id,
        s.species,
        e.locality,
        e.date AS collection_date,
        d.extraction_id,
        d.qubit_dna_ng_ul AS dna_concentration
    FROM SpecimenData AS s
    JOIN EventData AS e ON s.event_code = e.event_code
    JOIN DNAExtractions AS d ON s.lot_id = d.lot_id
    ORDER BY d.qubit_dna_ng_ul DESC
    LIMIT 8;
"""
)
rows = cur.fetchall()
print(
    f"{'Lot ID':<20} {'Species':<30} {'Locality':<15} {'Date':<15} {'Extraction':<12} {'DNA (ng/ul)':>12}"
)
print("-" * 105)
for row in rows:
    print(
        f"{str(row[0]):<20} {str(row[1]):<30} {str(row[2]):<15} {str(row[3]):<15} {str(row[4]):<12} {str(row[5]):>12}"
    )


# ---- Query 7: Count specimens per species (aggregate query) ----
cur.execute(
    """
    SELECT species, COUNT(*) AS specimen_count
    FROM SpecimenData
    GROUP BY species
    ORDER BY specimen_count DESC
    LIMIT 10;
"""
)
rows = cur.fetchall()
for row in rows:
    print(f"  {row[0]:<40} {row[1]} specimens")


# ---- Query 8: Specimens with voucher numbers----
cur.execute(
    """
    SELECT lot_id, species, voucher
    FROM SpecimenData
    WHERE voucher LIKE 'USNM%'
    ORDER BY voucher;
"""
)
rows = cur.fetchall()
for row in rows[:8]:
    print(row)
usnm_count = len(
    cur.execute("SELECT lot_id FROM SpecimenData WHERE voucher LIKE 'USNM%'").fetchall()
)
print(f"  ... ({usnm_count} total USNM vouchers)")

conn.close()
