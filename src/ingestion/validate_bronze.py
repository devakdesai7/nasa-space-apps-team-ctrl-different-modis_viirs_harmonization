
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = ROOT / "data" / "bronze"

EXPECTED_COLUMNS = [
    "latitude", "longitude", "brightness", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument",
    "confidence", "version", "bright_t31", "frp",
    "daynight", "type",
]

EXPECTED_METADATA = [
    "source_group", "source_file", "ingested_at_utc"
]

EXPECTED_GROUPS = ["MODIS", "NOAA20", "NOAA21", "SNPP"]


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

        total_records = 0

        for file in parquet_files:
            schema = con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(file)],
            ).fetchall()

            actual_columns = [row[0] for row in schema]
            actual_types = {row[0]: row[1] for row in schema}

            expected_columns = EXPECTED_COLUMNS + EXPECTED_METADATA

            if actual_columns != expected_columns:
                errors.append(f"Schema mismatch: {file.name}")
                continue

            for column in EXPECTED_COLUMNS:
                if actual_types[column] != "VARCHAR":
                    errors.append(
                        f"{file.name}: {column} is not VARCHAR"
                    )

            if actual_types["ingested_at_utc"] != "TIMESTAMP WITH TIME ZONE":
                errors.append(
                    f"{file.name}: incorrect ingestion timestamp type"
                )

            count = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(file)],
            ).fetchone()[0]

            total_records += count

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

            if metadata != [(file.parent.name, file.stem + ".csv", 1)]:
                errors.append(
                    f"{file.name}: metadata validation failed: {metadata}"
                )

            print(f"PASS: {file.name} ({count:,} records)")

        print(f"\nTotal Bronze records: {total_records:,}")

        # Check the known anomalous records.
        negative_frp = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE frp = '-5.9'
              AND acq_date = '2025-08-22'
              AND acq_time = '2126'
              AND satellite = 'Aqua'
            """,
            [str(BRONZE_ROOT / "MODIS" / "fire_archive_M-C61_846655.parquet")],
        ).fetchone()[0]

        zero_brightness = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE brightness = '0'
            """,
            [
                str(BRONZE_ROOT / "SNPP" / "fire_archive_SV-C2_846649.parquet")
            ],
        ).fetchone()[0]

        zero_brightness += con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE brightness = '0'
            """,
            [
                str(BRONZE_ROOT / "SNPP" / "fire_archive_SV-C2_846653.parquet")
            ],
        ).fetchone()[0]

        zero_brightness += con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE brightness = '0'
            """,
            [
                str(BRONZE_ROOT / "SNPP" / "fire_archive_SV-C2_846658.parquet")
            ],
        ).fetchone()[0]

        print(f"Negative FRP anomaly preserved: {negative_frp}")
        print(f"Zero brightness anomalies preserved: {zero_brightness}")

        if negative_frp != 1:
            errors.append("Negative FRP anomaly preservation failed")

        if zero_brightness != 29:
            errors.append("Zero brightness anomaly preservation failed")

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
