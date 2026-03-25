import sqlite3
import pandas as pd

# 1. SETUP PATHS
path = "LaPalma2023-Main-Dataset/"
db_path = 'panama_scripts/panama_specimens.db'

# 2. READ CSV FILES
event_df = pd.read_csv(path + "LaPalma2023-EventData.csv")
specimen_df = pd.read_csv(path + "LaPalma2023-SpecimenData.csv")
dna_df = pd.read_csv(path + "LaPalma2023-DNAextractions.csv")

# 3. GET MASTER COLUMN NAMES FROM DATABASE
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get EventData columns
cursor.execute('SELECT * FROM EventData LIMIT 0')
db_event_cols = [d[0] for d in cursor.description]

# Get SpecimenData columns
cursor.execute('SELECT * FROM SpecimenData LIMIT 0')
db_spec_cols = [d[0] for d in cursor.description]

# Get DNAExtractions columns
cursor.execute('SELECT * FROM DNAExtractions LIMIT 0')
db_dna_cols = [d[0] for d in cursor.description]

conn.close()

# 4. VALIDATION FUNCTION
def check_schema(table_name, db_columns, csv_columns):
    print(f"-- LaPalma: {table_name} --")
    for col in db_columns:
        if col not in csv_columns:
            print(f"MISSING: {col} -> Fix name or add column to CSV")
    print()

# 5. RUN CHECKS
check_schema("Event Table", db_event_cols, list(event_df.columns))
check_schema("Specimen Table", db_spec_cols, list(specimen_df.columns))
check_schema("DNA Table", db_dna_cols, list(dna_df.columns))