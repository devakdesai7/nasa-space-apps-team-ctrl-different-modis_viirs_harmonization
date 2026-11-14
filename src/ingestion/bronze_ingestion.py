
from datetime import datetime, timezone
from pathlib import Path

import argparse
import duckdb


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = ROOT / "NASA_DATA_FROM_2023_TO_2026"
BRONZE_ROOT = ROOT / "data" / "bronze"

EXPECTED_SOURCE_COLUMNS = [
    "latitude",
    "longitude",
    "brightness",
    "scan",
    "track",
    "acq_date",
    "acq_time",
    "satellite",
    "instrument",
    "confidence",
    "version",
    "bright_t31",
    "frp",
    "daynight",
    "type",
]

METADATA_COLUMNS = [
    "source_group",
    "source_file",
    "ingested_at_utc",
]

# The input folder uses NPP, while our standardized
# output folder name will be SNPP.
GROUP_MAPPING = {
    "MODIS": "MODIS",
    "NOAA20": "NOAA20",
    "NOAA21": "NOAA21",
    "NPP": "SNPP",
}


# --------------------------------------------------
# 2. Discover source files
# --------------------------------------------------

def discover_source_files():
    if not DATA_ROOT.exists():
        raise FileNotFoundError(
            f"Source data directory not found: {DATA_ROOT}"
        )

    source_files = sorted(DATA_ROOT.glob("*/*.csv"))

    if not source_files:
        raise FileNotFoundError(
            f"No CSV files found inside: {DATA_ROOT}"
        )

    return source_files


# --------------------------------------------------
# 3. Validate source schema
# --------------------------------------------------

def validate_source_schema(con, source_file):
    result = con.execute(
        """
        DESCRIBE
        SELECT *
        FROM read_csv(
            ?,
            header = true,
            all_varchar = true
        )
        """,
        [str(source_file)],
    ).fetchall()

    actual_columns = [row[0] for row in result]

    if actual_columns != EXPECTED_SOURCE_COLUMNS:
        raise ValueError(
            f"Unexpected schema in {source_file.name}\n"
            f"Expected: {EXPECTED_SOURCE_COLUMNS}\n"
            f"Actual:   {actual_columns}"
        )


# --------------------------------------------------
# 4. Count source records
# --------------------------------------------------

def count_source_records(con, source_file):
    return con.execute(
        """
        SELECT COUNT(*)
        FROM read_csv(
            ?,
            header = true,
            all_varchar = true
        )
        """,
        [str(source_file)],
    ).fetchone()[0]


# --------------------------------------------------
# 5. Ingest one source file
# --------------------------------------------------

def ingest_file(con, source_file, overwrite=False):
    input_group = source_file.parent.name.split("_")[0]

    if input_group not in GROUP_MAPPING:
        raise ValueError(
            f"Unknown source group: {input_group}"
        )

    source_group = GROUP_MAPPING[input_group]

    output_dir = BRONZE_ROOT / source_group
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{source_file.stem}.parquet"
    temp_file = output_dir / f"{source_file.stem}.parquet.tmp"

    if output_file.exists() and not overwrite:
        print(f"SKIPPED: {output_file.name} already exists")
        return

    validate_source_schema(con, source_file)

    source_count = count_source_records(con, source_file)

    ingested_at = datetime.now(timezone.utc).isoformat()

    output_sql_path = str(temp_file).replace("\\", "/")
    output_sql_path = output_sql_path.replace("'", "''")

    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    *,
                    ? AS source_group,
                    ? AS source_file,
                    CAST(? AS TIMESTAMPTZ) AS ingested_at_utc
                FROM read_csv(
                    ?,
                    header = true,
                    all_varchar = true
                )
            )
            TO '{output_sql_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """,
            [
                source_group,
                source_file.name,
                ingested_at,
                str(source_file),
            ],
        )

        # Validate the generated Parquet file.
        output_count = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            """,
            [str(temp_file)],
        ).fetchone()[0]

        if source_count != output_count:
            raise RuntimeError(
                f"Record count mismatch for {source_file.name}: "
                f"CSV={source_count}, Parquet={output_count}"
            )

        # Validate the output schema.
        output_schema = con.execute(
            """
            DESCRIBE
            SELECT *
            FROM read_parquet(?)
            """,
            [str(temp_file)],
        ).fetchall()

        output_columns = [row[0] for row in output_schema]
        expected_output_columns = (
            EXPECTED_SOURCE_COLUMNS + METADATA_COLUMNS
        )

        if output_columns != expected_output_columns:
            raise RuntimeError(
                f"Output schema mismatch for {source_file.name}\n"
                f"Expected: {expected_output_columns}\n"
                f"Actual:   {output_columns}"
            )

        # Publish the completed output file.
        temp_file.replace(output_file)

        print(f"\nSUCCESS: {source_file.name}")
        print(f"Source records: {source_count:,}")
        print(f"Output records: {output_count:,}")
        print(f"Output: {output_file}")

    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise


# --------------------------------------------------
# 6. Run the ingestion pipeline
# --------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest NASA FIRMS CSV files into Bronze Parquet."
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Recreate existing Bronze Parquet files.",
    )

    args = parser.parse_args()

    source_files = discover_source_files()

    print(f"Discovered {len(source_files)} source files.")

    con = duckdb.connect()
    con.execute("SET threads = 4")

    try:
        for source_file in source_files:
            ingest_file(
                con,
                source_file,
                overwrite=args.overwrite,
            )
    finally:
        con.close()

    print("\nBronze ingestion run completed.")


if __name__ == "__main__":
    main()
