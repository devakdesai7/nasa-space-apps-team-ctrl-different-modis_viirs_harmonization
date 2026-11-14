
from pathlib import Path
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "NASA_DATA_FROM_2023_TO_2026"
OUTPUT_FILE = ROOT / "profiling_stage2.json"

# Keep resource usage reasonable on a 16 GB machine.
con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 4")

files = sorted(DATA_ROOT.rglob("*.csv"))

print(f"Found {len(files)} CSV files")

# All original columns, used to identify exact duplicate rows.
ALL_COLUMNS = """
    latitude, longitude, brightness, scan, track,
    acq_date, acq_time, satellite, instrument, confidence,
    version, bright_t31, frp, daynight, type
"""

results = []

for index, file in enumerate(files, start=1):
    start = time.time()

    print(f"\n[{index}/{len(files)}] Profiling: {file.name}")

    # Read everything as text so source representations are preserved
    # during profiling. Numeric casts are performed only for analysis.
    params = [str(file)]

    # 1. Exact duplicate rows and invalid acquisition times.
    quality = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,

            COUNT(*) -
            COUNT(DISTINCT ({ALL_COLUMNS})) AS exact_duplicate_rows,

            SUM(
                CASE
                    WHEN regexp_full_match(trim(acq_time), '[0-9]{{1,4}}')
                     AND try_cast(
                         lpad(trim(acq_time), 4, '0') AS INTEGER
                     ) BETWEEN 0 AND 2359
                     AND (
                         try_cast(
                             lpad(trim(acq_time), 4, '0') AS INTEGER
                         ) % 100
                     ) < 60
                    THEN 0
                    ELSE 1
                END
            ) AS invalid_acq_time_rows

        FROM read_csv(?, header = true, all_varchar = true)
    """, params).fetchone()

    # 2. Distribution of instrument values.
    instrument_counts = con.execute("""
        SELECT instrument, COUNT(*) AS observations
        FROM read_csv(?, header = true, all_varchar = true)
        GROUP BY instrument
        ORDER BY instrument
    """, params).fetchall()

    # 3. Distribution of type values.
    type_counts = con.execute("""
        SELECT type, COUNT(*) AS observations
        FROM read_csv(?, header = true, all_varchar = true)
        GROUP BY type
        ORDER BY type
    """, params).fetchall()

    # 4. Numeric ranges and zero counts.
    numeric_stats = con.execute("""
        SELECT
            MIN(try_cast(brightness AS DOUBLE)) AS brightness_min,
            MAX(try_cast(brightness AS DOUBLE)) AS brightness_max,
            SUM(CASE WHEN try_cast(brightness AS DOUBLE) = 0
                     THEN 1 ELSE 0 END) AS brightness_zero_count,

            MIN(try_cast(scan AS DOUBLE)) AS scan_min,
            MAX(try_cast(scan AS DOUBLE)) AS scan_max,
            SUM(CASE WHEN try_cast(scan AS DOUBLE) = 0
                     THEN 1 ELSE 0 END) AS scan_zero_count,

            MIN(try_cast(track AS DOUBLE)) AS track_min,
            MAX(try_cast(track AS DOUBLE)) AS track_max,
            SUM(CASE WHEN try_cast(track AS DOUBLE) = 0
                     THEN 1 ELSE 0 END) AS track_zero_count,

            MIN(try_cast(frp AS DOUBLE)) AS frp_min,
            MAX(try_cast(frp AS DOUBLE)) AS frp_max,
            SUM(CASE WHEN try_cast(frp AS DOUBLE) = 0
                     THEN 1 ELSE 0 END) AS frp_zero_count

        FROM read_csv(?, header = true, all_varchar = true)
    """, params).fetchone()

    # 5. Retrieve complete records with negative FRP.
    negative_frp_rows = con.execute("""
        SELECT *
        FROM read_csv(?, header = true, all_varchar = true)
        WHERE try_cast(frp AS DOUBLE) < 0
        LIMIT 20
    """, params).fetchall()

    # Convert output into JSON-friendly Python objects.
    result = {
        "file": file.name,

        "total_rows": quality[0],
        "exact_duplicate_rows": quality[1],
        "invalid_acq_time_rows": quality[2],

        "instrument_counts": [
            {"instrument": row[0], "observations": row[1]}
            for row in instrument_counts
        ],

        "type_counts": [
            {"type": row[0], "observations": row[1]}
            for row in type_counts
        ],

        "numeric_statistics": {
            "brightness": {
                "min": numeric_stats[0],
                "max": numeric_stats[1],
                "zero_count": numeric_stats[2]
            },
            "scan": {
                "min": numeric_stats[3],
                "max": numeric_stats[4],
                "zero_count": numeric_stats[5]
            },
            "track": {
                "min": numeric_stats[6],
                "max": numeric_stats[7],
                "zero_count": numeric_stats[8]
            },
            "frp": {
                "min": numeric_stats[9],
                "max": numeric_stats[10],
                "zero_count": numeric_stats[11]
            }
        },

        "negative_frp_records": [
            dict(zip([column[0] for column in con.description],
                     row))
            for row in negative_frp_rows
        ]
    }

    results.append(result)

    print(f"Rows: {quality[0]:,}")
    print(f"Exact duplicate rows: {quality[1]:,}")
    print(f"Invalid acquisition times: {quality[2]:,}")

    print("\nInstrument distribution:")
    for row in instrument_counts:
        print(f"  {row[0]}: {row[1]:,}")

    print("\nType distribution:")
    for row in type_counts:
        print(f"  {row[0]}: {row[1]:,}")

    print("\nNumeric statistics:")
    print(json.dumps(result["numeric_statistics"], indent=2))

    if result["negative_frp_records"]:
        print("\nNEGATIVE FRP RECORDS:")
        print(json.dumps(
            result["negative_frp_records"],
            indent=2
        ))

    print(f"\nCompleted in {time.time() - start:.1f} seconds")

# Save the complete report.
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, default=str)

print("\n========== SECOND PROFILING STAGE COMPLETE ==========")
print(f"Report saved to: {OUTPUT_FILE}")

con.close()
