
from pathlib import Path

import duckdb


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = ROOT / "data" / "bronze"

EXPECTED_COLUMNS = [
    "latitude", "longitude", "brightness", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument",
    "confidence", "version", "bright_t31", "frp",
    "daynight", "type",
]

EXPECTED_METADATA = [
    "source_group",
    "source_file",
    "ingested_at_utc",
    "source_record_number",
]

EXPECTED_GROUPS = ["MODIS", "NOAA20", "NOAA21", "SNPP"]


# --------------------------------------------------
# 2. Validate one Parquet file
# --------------------------------------------------

def validate_file(con, file):
    errors = []

    # Read schema
    schema = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(file)],
    ).fetchall()

    actual_columns = [row[0] for row in schema]
    actual_types = {row[0]: row[1] for row in schema}

    expected_columns = EXPECTED_COLUMNS + EXPECTED_METADATA

    if actual_columns != expected_columns:
        errors.append(
            f"{file.name}: schema mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {actual_columns}"
        )
        return errors, 0

    # Validate original source column types
    for column in EXPECTED_COLUMNS:
        if actual_types[column] != "VARCHAR":
            errors.append(
                f"{file.name}: {column} is not VARCHAR"
            )

    # Validate metadata column types
    if actual_types["source_group"] != "VARCHAR":
        errors.append(
            f"{file.name}: source_group is not VARCHAR"
        )

    if actual_types["source_file"] != "VARCHAR":
        errors.append(
            f"{file.name}: source_file is not VARCHAR"
        )

    if actual_types["ingested_at_utc"] != "TIMESTAMP WITH TIME ZONE":
        errors.append(
            f"{file.name}: incorrect ingestion timestamp type"
        )

    if actual_types["source_record_number"] != "BIGINT":
        errors.append(
            f"{file.name}: source_record_number is not BIGINT"
        )

    # Count records
    count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(file)],
    ).fetchone()[0]

    # Validate metadata consistency
    metadata = con.execute(
        """
        SELECT
            source_group,
            source_file,
            COUNT(DISTINCT ingested_at_utc)
        FROM read_parquet(?)
        GROUP BY source_group, source_file
        """,
        [str(file)],
    ).fetchall()

    expected_metadata_values = [
        (file.parent.name, file.stem + ".csv", 1)
    ]

    if metadata != expected_metadata_values:
        errors.append(
            f"{file.name}: metadata validation failed: {metadata}"
        )

    # Validate source record numbering
    record_number_stats = con.execute(
        """
        SELECT
            COUNT(*) AS total_records,
            COUNT(source_record_number) AS non_null_records,
            COUNT(DISTINCT source_record_number) AS unique_records,
            MIN(source_record_number) AS min_record_number,
            MAX(source_record_number) AS max_record_number
        FROM read_parquet(?)
        """,
        [str(file)],
    ).fetchone()

    (
        total,
        non_null,
        unique,
        minimum,
        maximum,
    ) = record_number_stats

    if non_null != total:
        errors.append(
            f"{file.name}: NULL source record numbers found"
        )

    if unique != total:
        errors.append(
            f"{file.name}: duplicate source record numbers found"
        )

    if total > 0:
        if minimum != 1:
            errors.append(
                f"{file.name}: record numbering does not start at 1"
            )

        if maximum != total:
            errors.append(
                f"{file.name}: record numbering is not continuous"
            )

    return errors, count


# --------------------------------------------------
# 3. Run Bronze validation
# --------------------------------------------------

def main():
    con = duckdb.connect()
    errors = []

    try:
        parquet_files = sorted(BRONZE_ROOT.rglob("*.parquet"))

        print(f"Found {len(parquet_files)} Parquet files.")

        if len(parquet_files) != 12:
            errors.append(
                f"Expected 12 files, found {len(parquet_files)}"
            )

        # Validate output directories
        actual_groups = sorted(
            folder.name
            for folder in BRONZE_ROOT.iterdir()
            if folder.is_dir()
        ) if BRONZE_ROOT.exists() else []

        if actual_groups != sorted(EXPECTED_GROUPS):
            errors.append(
                f"Unexpected Bronze directories: {actual_groups}"
            )

        total_records = 0

        for file in parquet_files:
            file_errors, count = validate_file(con, file)

            total_records += count
            errors.extend(file_errors)

            if file_errors:
                print(f"FAIL: {file.name} ({count:,} records)")
            else:
                print(f"PASS: {file.name} ({count:,} records)")

        print(f"\nTotal Bronze records: {total_records:,}")

        # --------------------------------------------------
        # 4. Check known anomalous records
        # --------------------------------------------------

        negative_frp = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE frp = '-5.9'
              AND acq_date = '2025-08-22'
              AND acq_time = '2126'
              AND satellite = 'Aqua'
            """,
            [
                str(
                    BRONZE_ROOT
                    / "MODIS"
                    / "fire_archive_M-C61_846655.parquet"
                )
            ],
        ).fetchone()[0]

        zero_brightness = 0

        snpp_files = [
            "fire_archive_SV-C2_846649.parquet",
            "fire_archive_SV-C2_846653.parquet",
            "fire_archive_SV-C2_846658.parquet",
        ]

        for filename in snpp_files:
            zero_brightness += con.execute(
                """
                SELECT COUNT(*)
                FROM read_parquet(?)
                WHERE brightness = '0'
                """,
                [str(BRONZE_ROOT / "SNPP" / filename)],
            ).fetchone()[0]

        print(f"Negative FRP anomaly preserved: {negative_frp}")
        print(f"Zero brightness anomalies preserved: {zero_brightness}")

        if negative_frp != 1:
            errors.append(
                "Negative FRP anomaly preservation failed"
            )

        if zero_brightness != 29:
            errors.append(
                "Zero brightness anomaly preservation failed"
            )

        # --------------------------------------------------
        # 5. Final validation result
        # --------------------------------------------------

        if errors:
            print("\nVALIDATION FAILED:")

            for error in errors:
                print(f"- {error}")

        else:
            print("\nALL BRONZE VALIDATION CHECKS PASSED.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
    