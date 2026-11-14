
from pathlib import Path
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "NASA_DATA_FROM_2023_TO_2026"

con = duckdb.connect()

files = sorted(DATA_ROOT.rglob("*.csv"))

print(f"Found {len(files)} CSV files")

for index, file in enumerate(files, start=1):
    start = time.time()

    print(f"\n[{index}/{len(files)}] Profiling: {file.name}")

    relation = con.read_csv(
        str(file),
        header=True,
        all_varchar=True
    )

    # Main statistics: one full scan
    stats = relation.aggregate("""
        COUNT(*) AS row_count,

        MIN(acq_date) AS min_date,
        MAX(acq_date) AS max_date,

        COUNT(DISTINCT satellite) AS satellite_count,
        COUNT(DISTINCT confidence) AS confidence_count,
        COUNT(DISTINCT version) AS version_count,

        LIST(DISTINCT satellite) AS satellites,
        LIST(DISTINCT confidence) AS confidence_values,
        LIST(DISTINCT version) AS versions,

        SUM(CASE WHEN latitude IS NULL OR TRIM(latitude) = '' THEN 1 ELSE 0 END) AS missing_latitude,
        SUM(CASE WHEN longitude IS NULL OR TRIM(longitude) = '' THEN 1 ELSE 0 END) AS missing_longitude,
        SUM(CASE WHEN brightness IS NULL OR TRIM(brightness) = '' THEN 1 ELSE 0 END) AS missing_brightness,
        SUM(CASE WHEN scan IS NULL OR TRIM(scan) = '' THEN 1 ELSE 0 END) AS missing_scan,
        SUM(CASE WHEN track IS NULL OR TRIM(track) = '' THEN 1 ELSE 0 END) AS missing_track,
        SUM(CASE WHEN acq_date IS NULL OR TRIM(acq_date) = '' THEN 1 ELSE 0 END) AS missing_date,
        SUM(CASE WHEN acq_time IS NULL OR TRIM(acq_time) = '' THEN 1 ELSE 0 END) AS missing_time,
        SUM(CASE WHEN confidence IS NULL OR TRIM(confidence) = '' THEN 1 ELSE 0 END) AS missing_confidence,
        SUM(CASE WHEN frp IS NULL OR TRIM(frp) = '' THEN 1 ELSE 0 END) AS missing_frp
    """).fetchone()

    columns = [
        "row_count", "min_date", "max_date",
        "satellite_count", "confidence_count", "version_count",
        "satellites", "confidence_values", "versions",
        "missing_latitude", "missing_longitude",
        "missing_brightness", "missing_scan", "missing_track",
        "missing_date", "missing_time",
        "missing_confidence", "missing_frp"
    ]

    result = dict(zip(columns, stats))

    print("\nMAIN STATISTICS")
    print(json.dumps(result, indent=2, default=str))

    # Satellite-specific observation counts
    satellite_counts = con.execute("""
        SELECT
            satellite,
            COUNT(*) AS observations
        FROM read_csv(?, header = true, all_varchar = true)
        GROUP BY satellite
        ORDER BY satellite
    """, [str(file)]).fetchall()

    print("\nOBSERVATIONS BY SATELLITE")
    for satellite, count in satellite_counts:
        print(f"{satellite}: {count:,}")

    # Basic coordinate and measurement validity checks
    validity = relation.aggregate("""
        SUM(CASE WHEN TRY_CAST(latitude AS DOUBLE) NOT BETWEEN -90 AND 90 THEN 1 ELSE 0 END),
        SUM(CASE WHEN TRY_CAST(longitude AS DOUBLE) NOT BETWEEN -180 AND 180 THEN 1 ELSE 0 END),
        SUM(CASE WHEN TRY_CAST(scan AS DOUBLE) <= 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN TRY_CAST(track AS DOUBLE) <= 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN TRY_CAST(frp AS DOUBLE) < 0 THEN 1 ELSE 0 END)
    """).fetchone()

    print("\nINVALID VALUES")
    print({
        "invalid_latitude": validity[0],
        "invalid_longitude": validity[1],
        "nonpositive_scan": validity[2],
        "nonpositive_track": validity[3],
        "negative_frp": validity[4]
    })

    print(f"\nCompleted in {time.time() - start:.1f} seconds")

con.close()
print("\n========== PROFILING COMPLETE ==========")
