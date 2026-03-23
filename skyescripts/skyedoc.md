EventData
- 33 rows, 25 columns
- linking key is `event_code`
--- Event Data column names ---
['trip_id', 'event_code', 'fieldwork_status', 'year', 'month', 'day', 'date', 'waypoint', 'latitude', 'longitude', 'environment', 'low_tide', 'collecting_method', 'depth', 'population', 'locality', 'locality_details', 'city_district', 'province', 'area', 'photos_env', 'country', 'collector', 'notes', 'use_in_map']

Specimen Data
- 1,106 rows, 21 columns
- each specimen has a `lot_id`
- each specimen links to an `event_code`

--- Specimen Data column names ---
['lot_id', 'sufix', 'event_code', 'species', 'genus', 'epithet', 'clade', 'family', 'development', 'habitat', 'fixation_method', 'specimens', 'parts', 'vial', 'operculum', 'notes', 'photos_org', 'identification_by', 'Voucher', 'SecondVoucherClip', 'Unnamed: 20']  

DNAExtractions
- 782 rows, 19 columns
- each has an `extraction_id`
- each links to a `lot_id`

--- DNA Extraction column names ---
['extraction_id', 'lot_id', 'species', 'plate_id', 'plate_well', 'extraction_date', 'extraction_kit', 'elution_ul', 'Qubit_DNA_[ng/ul]', 'Nanodrop_[ng/ul]', 'Nanodrop_260/280', 'Nanodrop_260/230', 'Qubit : Nanodrop', 'clip_over', 'contamination_pla'Qubit_after_SpeedVac']

Genomic Libraries
- 11 rows, 24 columns
- each references an `extraction_id`

--- Genomic Library column names ---
['library_id', 'extraction_id', 'lot_id', 'species', 'library_date', 'library_kit', 'Qubit_DNA_[ng/ul]', 'input_mass_ng', 'input_vol_ul', 'EB_complement_ul', 'frag_time_min', 'cycles', 'elution_ul', 'Qubit_lib_[ng/ul]', 'size_selection', 'Qubit_size-selection_[ng/ul]', 'IDT xGen UDI Primer Pair Well', 'Primer Name', 'i5 index', 'i7 index', 'Bioanalyzer_avg_size', 'insert_size', '[C]_nM_estimate', 'Data_Target_GB']

**Event->Specimen->Extraction->Library**

SQLite stores entire database as a single `.db` file

1. Load and clean CSVs with pandas
2. Create database with proper links
3. Load all 4 Panama datasets
4. Test queries

[DB Browser for SQLite](https://sqlitebrowser.org) -- possible early-stage GUI

### Relational Databases

A relational database stores data in multiple tables that are LINKED to each other through shared ID columns called "keys." This avoids storing the same information in multiple places (redundancy).
 
In our database, the links work like this:

    EventData   <-- (event_code) --> SpecimenData
    SpecimenData <-- (lot_id)    --> DNAExtractions
    DNAExtractions <-- (extraction_id) --> GenomicLibraries
 
So if you want to know WHERE a DNA extraction came from geographically,
you only need to store the lot_id in the extractions table.
You can always "link" back to EventData to get the GPS coordinates. 

**Schema are the rules the db uses**

PRIMARY KEY: every row has a unique identifier
FOREIGN KEY: a column in one table must match a valid value in another table
Data types: TEXT, INTEGER, REAL, etc

NOT NULL: this column must always have a value (can't be empty)
TEXT:stores text/string data
REAL:stores decimal numbers (floating point)
INTEGER:stores whole numbers

**Querying**

WHERE: filter by a condition
JOIN ... ON: combine information across tables by matching shared ID columns
GROUP BY with COUNT(*): lets you ask aggregate questions e.g. "how many specimens per species?"
LIKE 'USNM%': pattern match, where % means "anything after this"

## Cleaned Columns
- normalized column names
- removed use_in_map and population from EventData
- removed unnamed columns from SpecimenData


Cleaned Event columns:    ['trip_id', 'event_code', 'fieldwork_status', 'year', 'month', 'day', 'date', 'waypoint', 'latitude', 'longitude', 'environment', 'low_tide', 'collecting_method', 'depth', 'locality', 'locality_details', 'city_district', 'province', 'area', 'photos_env', 'country', 'collector', 'event_notes']

Cleaned Specimen columns: ['lot_id', 'suffix', 'event_code', 'species', 'genus', 'epithet', 'clade', 'family', 'development', 'habitat', 'fixation_method', 'specimen_count', 'parts', 'vial', 'operculum', 'specimen_notes', 'photos_org', 'identification_by', 'voucher', 'second_voucher_clip']

Cleaned DNA columns: ['extraction_id', 'lot_id', 'species', 'plate_id', 'plate_well', 'extraction_date', 'extraction_kit', 'elution_ul', 'qubit_dna_ng_ul', 'nanodrop_ng_ul', 'nanodrop_260_280', 'nanodrop_260_230', 'qubit_nanodrop_ratio', 'clip_over', 'contamination_plate', 'contamination_wells', 'extraction_notes', 'piece_size', 'qubit_after_speedvac']

Cleaned Genomic Library columns: ['library_id', 'extraction_id', 'lot_id', 'species', 'library_date', 'library_kit', 'qubit_dna_ng_ul', 'input_mass_ng', 'input_vol_ul', 'eb_complement_ul', 'frag_time_min', 'cycles', 'elution_ul', 'qubit_lib_ng_ul', 'size_selection', 'qubit_size_selection_ng_ul', 'idt_primer_well', 'primer_name', 'i5_index', 'i7_index', 'bioanalyzer_avg_size', 'insert_size', 'concentration_nm_estimate', 'data_target_gb']