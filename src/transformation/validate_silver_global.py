
import sys
from pathlib import Path

import duckdb


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
TEMP_ROOT = PROJECT_ROOT / "data" / "temp" / "duckdb_global_validation"

EXPECTED_FILE_COUNT = 12
EXPECTED_TOTAL_RECORDS = 196_335_668

EXPECTED_GROUP_COUNTS = {
    "MODIS": 13_112_124,
    "NOAA20": 62_094_904,
    "NOAA21": 60_079_396,
    "SNPP": 61_049_244,
}

EXPECTED_FILES_PER_GROUP = 3

EXPECTED_MIN_DATE = "2023-03-01"
EXPECTED_MAX_DATE = "2026-02-28"

EXPECTED_QUALITY_COUNTS = (1, 29, 0, 0, 0, 30)


# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

def sql_string(value: str) -> str:
    """Safely quote a string for use as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


# ---------------------------------------------------------
# Main validation
# ---------------------------------------------------------

def main() -> int:

    TEMP_ROOT.mkdir(parents=True, exist_ok=True)

    silver_files = sorted(SILVER_ROOT.glob("*/*.parquet"))

    failures = []

    print_section("GLOBAL SILVER VALIDATION")

    print(f"Silver directory : {SILVER_ROOT}")
    print(f"Files discovered : {len(silver_files)}")
    print(f"Expected files   : {EXPECTED_FILE_COUNT}")

    # -----------------------------------------------------
    # 1. File completeness
    # -----------------------------------------------------

    print_section("FILE COMPLETENESS")

    if len(silver_files) == EXPECTED_FILE_COUNT:
        print("[PASS] Expected number of Silver files found.")
    else:
        failures.append(
            f"Expected {EXPECTED_FILE_COUNT} files, "
            f"but found {len(silver_files)}."
        )

    for group in EXPECTED_GROUP_COUNTS:

        group_files = [
            path for path in silver_files
            if path.parent.name.upper() == group
        ]

        if len(group_files) == EXPECTED_FILES_PER_GROUP:
            print(f"[PASS] {group}: {EXPECTED_FILES_PER_GROUP} files found.")
        else:
            failures.append(
                f"{group}: expected {EXPECTED_FILES_PER_GROUP} files, "
                f"found {len(group_files)}."
            )

    if not silver_files:
        print("\nNo Silver files found. Cannot continue.")
        return 1

    # -----------------------------------------------------
    # 2. Global identity namespace check
    # -----------------------------------------------------

    print_section("GLOBAL IDENTITY NAMESPACE")

    # Each observation ID is generated using:
    # source_group + filename stem + source_record_number.
    #
    # Individual Silver validation already checks:
    # - observation_id uniqueness within each file
    # - source_record_number uniqueness within each file
    #
    # Here we verify that every file has a distinct
    # (source_group, filename stem) namespace.

    file_namespaces = [
        (path.parent.name.upper(), path.stem)
        for path in silver_files
    ]

    if len(file_namespaces) == len(set(file_namespaces)):
        print("[PASS] Every Silver file has a unique identity namespace.")
    else:
        failures.append(
            "Duplicate Silver file identity namespaces detected."
        )

    # -----------------------------------------------------
    # 3. Prepare DuckDB
    # -----------------------------------------------------

    file_list_sql = ", ".join(
        sql_string(
            str(path.resolve()).replace("\\", "/")
        )
        for path in silver_files
    )

    connection = duckdb.connect()

    try:

        connection.execute("SET threads = 4")
        connection.execute("SET memory_limit = '4GB'")
        connection.execute(
            f"SET temp_directory = "
            f"{sql_string(str(TEMP_ROOT.resolve()).replace(chr(92), '/'))}"
        )
        connection.execute("SET TimeZone = 'UTC'")

        source = f"read_parquet([{file_list_sql}])"

        # -------------------------------------------------
        # 4. Global dataset metrics
        # -------------------------------------------------

        print_section("GLOBAL DATASET METRICS")

        # Avoid COUNT(DISTINCT) across 196 million rows.
        # Individual file validators establish uniqueness.
        # This query checks global counts, date range and nulls.

        metrics = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_records,

                MIN(acq_date) AS min_acq_date,
                MAX(acq_date) AS max_acq_date,

                COUNT(*) FILTER (
                    WHERE observation_id IS NULL
                ) AS null_observation_ids,

                COUNT(*) FILTER (
                    WHERE source_file IS NULL
                ) AS null_source_files,

                COUNT(*) FILTER (
                    WHERE source_record_number IS NULL
                ) AS null_source_record_numbers

            FROM {source}
            """
        ).fetchone()

        (
            total_records,
            min_date,
            max_date,
            null_ids,
            null_source_files,
            null_source_records,
        ) = metrics

        print(f"Total records          : {total_records:,}")
        print(f"Acquisition date range : {min_date} to {max_date}")
        print(f"Null observation IDs   : {null_ids:,}")
        print(f"Null source files      : {null_source_files:,}")
        print(f"Null source row numbers: {null_source_records:,}")

        # Total records

        if total_records == EXPECTED_TOTAL_RECORDS:
            print("[PASS] Total record count matches expected count.")
        else:
            failures.append(
                f"Expected {EXPECTED_TOTAL_RECORDS:,} records, "
                f"found {total_records:,}."
            )

        # Null observation IDs

        if null_ids == 0:
            print("[PASS] No null observation IDs.")
        else:
            failures.append(
                f"Found {null_ids:,} null observation IDs."
            )

        # Null provenance

        if null_source_files == 0 and null_source_records == 0:
            print("[PASS] Source provenance fields are non-null.")
        else:
            failures.append(
                "Null source provenance fields detected."
            )

        # Acquisition date range

        if (
            str(min_date) == EXPECTED_MIN_DATE
            and str(max_date) == EXPECTED_MAX_DATE
        ):
            print("[PASS] Acquisition date coverage matches expected range.")
        else:
            failures.append(
                f"Unexpected acquisition date range: "
                f"{min_date} to {max_date}."
            )

        # -------------------------------------------------
        # 5. Counts by source group
        # -------------------------------------------------

        print_section("RECORD COUNTS BY SOURCE GROUP")

        group_rows = connection.execute(
            f"""
            SELECT
                source_group,
                COUNT(*) AS record_count
            FROM {source}
            GROUP BY source_group
            ORDER BY source_group
            """
        ).fetchall()

        observed_groups = {}

        for group, count in group_rows:

            observed_groups[group] = count
            expected = EXPECTED_GROUP_COUNTS.get(group)

            if expected is not None and count == expected:
                status = "PASS"
            else:
                status = "FAIL"

            if expected is not None:
                print(
                    f"{group:<10} {count:>15,} "
                    f"(expected: {expected:,}) [{status}]"
                )
            else:
                print(
                    f"{group:<10} {count:>15,} "
                    f"[UNEXPECTED GROUP]"
                )

            if expected is None or count != expected:
                failures.append(
                    f"{group}: expected {expected}, found {count}."
                )

        # Check for missing source groups

        for group in EXPECTED_GROUP_COUNTS:

            if group not in observed_groups:
                failures.append(
                    f"Missing source group: {group}."
                )

        # -------------------------------------------------
        # 6. Counts by satellite platform
        # -------------------------------------------------

        print_section("RECORD COUNTS BY SATELLITE PLATFORM")

        platform_rows = connection.execute(
            f"""
            SELECT
                satellite_platform,
                sensor_family,
                COUNT(*) AS record_count
            FROM {source}
            GROUP BY
                satellite_platform,
                sensor_family
            ORDER BY satellite_platform
            """
        ).fetchall()

        for platform, sensor, count in platform_rows:

            print(
                f"{platform:<12} "
                f"{sensor:<8} "
                f"{count:>15,}"
            )

        # -------------------------------------------------
        # 7. Dataset-wide quality audit
        # -------------------------------------------------

        print_section("DATASET-WIDE QUALITY AUDIT")

        quality_rows = connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE is_negative_frp
                ) AS negative_frp,

                COUNT(*) FILTER (
                    WHERE is_zero_brightness
                ) AS zero_brightness,

                COUNT(*) FILTER (
                    WHERE has_parse_issue
                ) AS parse_issues,

                COUNT(*) FILTER (
                    WHERE has_timestamp_issue
                ) AS timestamp_issues,

                COUNT(*) FILTER (
                    WHERE has_standardization_issue
                ) AS standardization_issues,

                COUNT(*) FILTER (
                    WHERE has_quality_issue
                ) AS total_quality_issues

            FROM {source}
            """
        ).fetchone()

        quality_names = [
            "Negative FRP",
            "Zero brightness",
            "Parse issues",
            "Timestamp issues",
            "Standardization issues",
            "Total quality issues",
        ]

        for name, count in zip(quality_names, quality_rows):

            print(
                f"{name:<28}: {count:,}"
            )

        if quality_rows == EXPECTED_QUALITY_COUNTS:

            print(
                "[PASS] Quality counts match the previous Silver audit."
            )

        else:

            print(
                "[INFO] Quality counts differ from the previous audit. "
                "Investigate before treating this as a failure."
            )

        # -------------------------------------------------
        # 8. Final result
        # -------------------------------------------------

        print_section("VALIDATION RESULT")

        if failures:

            print(
                f"[FAIL] {len(failures)} validation issue(s):"
            )

            for index, failure in enumerate(failures, start=1):
                print(f"{index}. {failure}")

            return 1

        print(
            "[PASS] All global Silver validation checks passed."
        )

        return 0

    finally:

        connection.close()


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
