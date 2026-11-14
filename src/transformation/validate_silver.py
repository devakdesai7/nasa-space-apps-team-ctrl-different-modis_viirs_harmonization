
import argparse
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"

SOURCE_GROUPS = ("MODIS", "NOAA20", "NOAA21", "SNPP")

# Expected Silver schema: column name -> DuckDB data type.
EXPECTED_SCHEMA = {
    "observation_id": "VARCHAR",
    "source_group": "VARCHAR",
    "source_file": "VARCHAR",
    "source_record_number": "BIGINT",
    "ingested_at_utc": "TIMESTAMP WITH TIME ZONE",
    "satellite_raw": "VARCHAR",
    "satellite_platform": "VARCHAR",
    "sensor_family": "VARCHAR",
    "instrument_raw": "VARCHAR",
    "version_raw": "VARCHAR",
    "collection_version": "VARCHAR",
    "acq_date": "DATE",
    "acq_time_raw": "VARCHAR",
    "acq_timestamp_utc": "TIMESTAMP WITH TIME ZONE",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "brightness": "DOUBLE",
    "bright_t31": "DOUBLE",
    "frp": "DOUBLE",
    "scan": "DOUBLE",
    "track": "DOUBLE",
    "daynight": "VARCHAR",
    "confidence_raw": "VARCHAR",
    "confidence_numeric": "DOUBLE",
    "confidence_category": "VARCHAR",
    "type_raw": "VARCHAR",
    "is_negative_frp": "BOOLEAN",
    "is_zero_brightness": "BOOLEAN",
    "has_parse_issue": "BOOLEAN",
    "has_timestamp_issue": "BOOLEAN",
    "has_standardization_issue": "BOOLEAN",
    "has_quality_issue": "BOOLEAN",
}

EXPECTED_SENSOR_FAMILY = {
    "MODIS": "MODIS",
    "NOAA20": "VIIRS",
    "NOAA21": "VIIRS",
    "SNPP": "VIIRS",
}


def get_scalar(con, query, params=None):
    """Execute a query that returns one value."""
    return con.execute(query, params or []).fetchone()[0]


def validate_file(
    con,
    silver_file: Path,
    group: str,
    bronze_root: Path,
) -> bool:
    print(f"\nValidating: {silver_file.name}")

    failures = 0

    def check(description, passed, details=""):
        nonlocal failures

        if passed:
            print(f"  PASS: {description}")
        else:
            failures += 1
            suffix = f" ({details})" if details else ""
            print(f"  FAIL: {description}{suffix}")

    # --------------------------------------------------
    # 1. File existence and expected Bronze counterpart
    # --------------------------------------------------

    check("Silver file exists", silver_file.is_file())

    if not silver_file.is_file():
        return False

    bronze_file = bronze_root / group / silver_file.name

    check(
        "Matching Bronze file exists",
        bronze_file.is_file(),
        str(bronze_file),
    )

    if not bronze_file.is_file():
        return False

    silver_path = str(silver_file.resolve())
    bronze_path = str(bronze_file.resolve())

    # Use a UTC session for consistent timestamp comparisons.
    con.execute("SET TimeZone = 'UTC'")

    # --------------------------------------------------
    # 2. Schema validation
    # --------------------------------------------------

    schema_rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [silver_path],
    ).fetchall()

    actual_schema = {
        row[0]: row[1].upper()
        for row in schema_rows
    }

    expected_schema = EXPECTED_SCHEMA

    check(
        "Exactly 32 columns",
        len(schema_rows) == len(expected_schema),
        f"Found {len(schema_rows)}",
    )

    check(
        "Column names and order match expected schema",
        [row[0] for row in schema_rows]
        == list(expected_schema.keys()),
    )

    type_errors = []

    for column, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column)

        if actual_type != expected_type:
            type_errors.append(
                f"{column}: expected {expected_type}, found {actual_type}"
            )

    check(
        "All column data types are correct",
        len(type_errors) == 0,
        "; ".join(type_errors),
    )

    # --------------------------------------------------
    # 3. Record count and Bronze consistency
    # --------------------------------------------------

    silver_count = get_scalar(
        con,
        "SELECT COUNT(*) FROM read_parquet(?)",
        [silver_path],
    )

    bronze_count = get_scalar(
        con,
        "SELECT COUNT(*) FROM read_parquet(?)",
        [bronze_path],
    )

    check(
        "Silver record count matches Bronze",
        silver_count == bronze_count,
        f"Silver={silver_count}, Bronze={bronze_count}",
    )

    print(f"  Silver records: {silver_count:,}")
    print(f"  Bronze records: {bronze_count:,}")

    # --------------------------------------------------
    # 4. Observation ID validation
    # --------------------------------------------------

    id_stats = con.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(observation_id) AS non_null_ids,
            COUNT(DISTINCT observation_id) AS unique_ids
        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    total, non_null_ids, unique_ids = id_stats

    check(
        "Observation IDs are non-null",
        total == non_null_ids,
    )

    check(
        "Observation IDs are unique",
        total == unique_ids,
    )

    # --------------------------------------------------
    # 5. Source record number validation
    # --------------------------------------------------

    record_stats = con.execute(
        """
        SELECT
            COUNT(source_record_number),
            COUNT(DISTINCT source_record_number),
            MIN(source_record_number),
            MAX(source_record_number)
        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    non_null_records, unique_records, min_record, max_record = record_stats

    check(
        "Source record numbers are non-null",
        non_null_records == silver_count,
    )

    check(
        "Source record numbers are unique",
        unique_records == silver_count,
    )

    check(
        "Source record numbers form a continuous sequence",
        min_record == 1 and max_record == silver_count,
        f"Min={min_record}, Max={max_record}",
    )

    # --------------------------------------------------
    # 6. Provenance validation
    # --------------------------------------------------

    provenance = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE source_group IS NULL),
            COUNT(*) FILTER (WHERE source_file IS NULL),
            COUNT(*) FILTER (WHERE ingested_at_utc IS NULL),
            COUNT(DISTINCT source_group),
            COUNT(DISTINCT source_file)
        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    (
        null_groups,
        null_files,
        null_ingestion_times,
        distinct_groups,
        distinct_files,
    ) = provenance

    check("Source group is never null", null_groups == 0)
    check("Source file is never null", null_files == 0)
    check("Ingestion timestamp is never null", null_ingestion_times == 0)

    check(
        "All records belong to the expected source group",
        distinct_groups == 1
        and get_scalar(
            con,
            "SELECT MIN(source_group) FROM read_parquet(?)",
            [silver_path],
        ) == group,
    )

    check(
        "Exactly one source file is represented",
        distinct_files == 1,
    )

    # --------------------------------------------------
    # 7. Sensor family and collection validation
    # --------------------------------------------------

    expected_family = EXPECTED_SENSOR_FAMILY[group]

    sensor_stats = con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE sensor_family IS NULL
                   OR sensor_family != ?
            ),
            COUNT(*) FILTER (
                WHERE collection_version IS NULL
            )
        FROM read_parquet(?)
        """,
        [expected_family, silver_path],
    ).fetchone()

    invalid_sensor_family, missing_collection = sensor_stats

    check(
        "Sensor family is correctly standardized",
        invalid_sensor_family == 0,
    )

    check(
        "Collection version is populated",
        missing_collection == 0,
    )

    # --------------------------------------------------
    # 8. Timestamp validation
    # --------------------------------------------------

    timestamp_stats = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE acq_date IS NULL),
            COUNT(*) FILTER (WHERE acq_time_raw IS NULL),
            COUNT(*) FILTER (WHERE acq_timestamp_utc IS NULL),
            COUNT(*) FILTER (
                WHERE strftime(
                    acq_timestamp_utc,
                    '%Y-%m-%d%H%M'
                ) != (
                    CAST(acq_date AS VARCHAR)
                    || LPAD(TRIM(acq_time_raw), 4, '0')
                )
            )
        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    (
        missing_dates,
        missing_raw_times,
        missing_timestamps,
        timestamp_mismatches,
    ) = timestamp_stats

    check("Acquisition dates are populated", missing_dates == 0)
    check("Raw acquisition times are populated", missing_raw_times == 0)
    check("Parsed timestamps are populated", missing_timestamps == 0)
    check("Parsed timestamps match raw date/time", timestamp_mismatches == 0)

    # --------------------------------------------------
    # 9. Confidence semantics
    # --------------------------------------------------

    confidence_stats = con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE confidence_raw IS NULL
            ),
            COUNT(*) FILTER (
                WHERE confidence_numeric IS NOT NULL
            ),
            COUNT(*) FILTER (
                WHERE confidence_category IS NOT NULL
            ),
            COUNT(*) FILTER (
                WHERE confidence_category IS NOT NULL
                  AND LOWER(confidence_category) NOT IN ('l', 'n', 'h')
            )
        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    (
        missing_confidence,
        numeric_confidence,
        categorical_confidence,
        invalid_categories,
    ) = confidence_stats

    check(
        "Raw confidence is populated",
        missing_confidence == 0,
    )

    if group == "MODIS":
        check(
            "MODIS confidence is numeric",
            numeric_confidence == silver_count,
        )
        check(
            "MODIS categorical confidence is null",
            categorical_confidence == 0,
        )

        confidence_range_issues = get_scalar(
            con,
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE confidence_numeric < 0
               OR confidence_numeric > 100
            """,
            [silver_path],
        )

        check(
            "MODIS confidence is within 0–100",
            confidence_range_issues == 0,
        )

    else:
        check(
            "VIIRS numeric confidence is null",
            numeric_confidence == 0,
        )
        check(
            "VIIRS categorical confidence is populated",
            categorical_confidence == silver_count,
        )
        check(
            "VIIRS confidence categories are valid",
            invalid_categories == 0,
        )

    # --------------------------------------------------
    # 10. Quality flag validation
    # --------------------------------------------------

    quality_stats = con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE is_negative_frp IS NULL
                   OR is_zero_brightness IS NULL
                   OR has_parse_issue IS NULL
                   OR has_timestamp_issue IS NULL
                   OR has_standardization_issue IS NULL
                   OR has_quality_issue IS NULL
            ),

            COUNT(*) FILTER (
                WHERE has_quality_issue != (
                    is_negative_frp
                    OR is_zero_brightness
                    OR has_parse_issue
                    OR has_timestamp_issue
                    OR has_standardization_issue
                )
            ),

            COUNT(*) FILTER (WHERE is_negative_frp),
            COUNT(*) FILTER (WHERE is_zero_brightness),
            COUNT(*) FILTER (WHERE has_parse_issue),
            COUNT(*) FILTER (WHERE has_timestamp_issue),
            COUNT(*) FILTER (WHERE has_standardization_issue),
            COUNT(*) FILTER (WHERE has_quality_issue)

        FROM read_parquet(?)
        """,
        [silver_path],
    ).fetchone()

    (
        null_quality_flags,
        inconsistent_quality_flags,
        negative_frp,
        zero_brightness,
        parse_issues,
        timestamp_issues,
        standardization_issues,
        total_quality_issues,
    ) = quality_stats

    check(
        "All quality flags are non-null",
        null_quality_flags == 0,
    )

    check(
        "Overall quality flag matches individual flags",
        inconsistent_quality_flags == 0,
    )

    print("  Quality flag counts:")
    print(f"    Negative FRP:          {negative_frp:,}")
    print(f"    Zero brightness:       {zero_brightness:,}")
    print(f"    Parse issues:          {parse_issues:,}")
    print(f"    Timestamp issues:      {timestamp_issues:,}")
    print(f"    Standardization issues:{standardization_issues:,}")
    print(f"    Overall quality:      {total_quality_issues:,}")

    # --------------------------------------------------
    # Final result
    # --------------------------------------------------

    if failures == 0:
        print("\n  RESULT: PASS")
        return True

    print(f"\n  RESULT: FAIL ({failures} failed checks)")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Validate NASA FIRMS Silver Parquet files."
    )

    parser.add_argument(
        "--group",
        choices=SOURCE_GROUPS,
        help="Validate one source group.",
    )

    parser.add_argument(
        "--file",
        help="Validate one Parquet file by filename.",
    )

    parser.add_argument(
        "--silver-root",
        type=Path,
        default=DEFAULT_SILVER_ROOT,
    )

    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=DEFAULT_BRONZE_ROOT,
    )

    args = parser.parse_args()

    if args.file and not args.group:
        parser.error("--file requires --group")

    if args.group:
        groups = [args.group]
    else:
        groups = list(SOURCE_GROUPS)

    files_to_validate = []

    for group in groups:
        group_dir = args.silver_root / group

        if args.file:
            candidate = group_dir / args.file
            if candidate.is_file():
                files_to_validate.append((group, candidate))
            else:
                print(f"File not found: {candidate}")
        elif group_dir.exists():
            for path in sorted(group_dir.glob("*.parquet")):
                files_to_validate.append((group, path))

    if not files_to_validate:
        print("No Silver Parquet files found to validate.")
        raise SystemExit(1)

    con = duckdb.connect()
    results = []

    try:
        for group, silver_file in files_to_validate:
            passed = validate_file(
                con,
                silver_file,
                group,
                args.bronze_root,
            )
            results.append((silver_file.name, passed))
    finally:
        con.close()

    print("\n" + "=" * 55)
    print("SILVER VALIDATION SUMMARY")
    print("=" * 55)

    for filename, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{status}: {filename}")

    passed_count = sum(passed for _, passed in results)
    failed_count = len(results) - passed_count

    print(f"\nFiles checked: {len(results)}")
    print(f"Passed:       {passed_count}")
    print(f"Failed:       {failed_count}")

    if failed_count:
        raise SystemExit(1)

    print("\nALL SILVER VALIDATION CHECKS PASSED.")


if __name__ == "__main__":
    main()
