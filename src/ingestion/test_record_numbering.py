
from pathlib import Path
import duckdb

# Project root
ROOT = Path(__file__).resolve().parents[2]

# Select the first CSV file
csv_file = next(
    (ROOT / "NASA_DATA_FROM_2023_TO_2026").glob("*/*.csv")
)

print(f"Testing file: {csv_file.name}")

# Connect to DuckDB
con = duckdb.connect()
con.execute("SET preserve_insertion_order = true")

# Generate row numbers and calculate validation statistics
query = f"""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_record_number) AS unique_records,
    MIN(source_record_number) AS min_record_number,
    MAX(source_record_number) AS max_record_number
FROM (
    SELECT
        row_number() OVER () AS source_record_number
    FROM read_csv(
        '{csv_file.as_posix()}',
        header = true,
        all_varchar = true
    )
)
"""

result = con.execute(query).fetchone()

# Display results
total_records, unique_records, min_record, max_record = result

print("\nRecord Numbering Validation")
print("---------------------------")
print(f"Total records:       {total_records:,}")
print(f"Unique record nums:  {unique_records:,}")
print(f"Minimum record num:  {min_record}")
print(f"Maximum record num:  {max_record:,}")

# Validate
if (
    total_records == unique_records
    and min_record == 1
    and max_record == total_records
):
    print("\nPASS: Record numbering is unique and continuous.")
else:
    print("\nFAIL: Record numbering validation failed.")

con.close()
