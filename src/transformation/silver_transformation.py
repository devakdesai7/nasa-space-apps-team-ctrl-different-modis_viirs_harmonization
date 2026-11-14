
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"


def sql_string(value: str) -> str:
    """Safely quote a string literal for use in DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def transform_file(
    input_file: Path,
    output_file: Path,
    source_group: str,
) -> None:
    """Transform one Bronze Parquet file into a Silver Parquet file."""

    if not input_file.exists():
        raise FileNotFoundError(f"Bronze file not found: {input_file}")

    if source_group not in {"MODIS", "NOAA20", "NOAA21", "SNPP"}:
        raise ValueError(f"Unknown source group: {source_group}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # The source filename stem plus original row number gives each
    # observation a stable identity within this source dataset.
    id_prefix = f"{source_group}_{input_file.stem}"

    input_sql = sql_string(input_file.resolve().as_posix())
    temp_file = output_file.with_suffix(output_file.suffix + ".tmp")
    temp_sql = sql_string(temp_file.resolve().as_posix())

    if temp_file.exists():
        temp_file.unlink()

    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute("SET TimeZone = 'UTC'")

    try:
        query = f"""
        COPY (
            WITH parsed AS (
                SELECT
                    source_group,
                    source_file,
                    ingested_at_utc,
                    source_record_number,

                    satellite AS satellite_raw,
                    instrument AS instrument_raw,
                    version AS version_raw,

                    TRY_CAST(acq_date AS DATE) AS acq_date,
                    acq_time AS acq_time_raw,

                    TRY_CAST(latitude AS DOUBLE) AS latitude,
                    TRY_CAST(longitude AS DOUBLE) AS longitude,
                    TRY_CAST(brightness AS DOUBLE) AS brightness,
                    TRY_CAST(bright_t31 AS DOUBLE) AS bright_t31,
                    TRY_CAST(frp AS DOUBLE) AS frp,
                    TRY_CAST(scan AS DOUBLE) AS scan,
                    TRY_CAST(track AS DOUBLE) AS track,

                    daynight,
                    confidence AS confidence_raw,
                    type AS type_raw,

                    TRY_STRPTIME(
                        TRIM(acq_date) || LPAD(TRIM(acq_time), 4, '0'),
                        '%Y-%m-%d%H%M'
                    ) AS parsed_acq_timestamp

                FROM read_parquet({input_sql})
            ),

            standardized AS (
                SELECT
                    *,

                    CASE UPPER(TRIM(satellite_raw))
                        WHEN 'TERRA' THEN 'TERRA'
                        WHEN 'AQUA' THEN 'AQUA'
                        WHEN 'N20' THEN 'NOAA20'
                        WHEN 'N21' THEN 'NOAA21'
                        WHEN 'SNPP' THEN 'SNPP'
                        ELSE NULL
                    END AS satellite_platform,

                    CASE
                        WHEN source_group = 'MODIS' THEN 'MODIS'
                        WHEN source_group IN ('NOAA20', 'NOAA21', 'SNPP')
                            THEN 'VIIRS'
                        ELSE NULL
                    END AS sensor_family,

                    CASE
                        WHEN TRIM(version_raw) LIKE '61%' THEN 'C61'
                        WHEN TRIM(version_raw) = '2' THEN 'C2'
                        ELSE NULL
                    END AS collection_version,

                    timezone('UTC', parsed_acq_timestamp)
                        AS acq_timestamp_utc,

                    CASE
                        WHEN source_group = 'MODIS'
                            THEN TRY_CAST(confidence_raw AS DOUBLE)
                        ELSE NULL
                    END AS confidence_numeric,

                    CASE
                        WHEN source_group IN ('NOAA20', 'NOAA21', 'SNPP')
                            THEN LOWER(TRIM(confidence_raw))
                        ELSE NULL
                    END AS confidence_category

                FROM parsed
            ),

            flagged AS (
                SELECT
                    *,

                    COALESCE(frp < 0, FALSE) AS is_negative_frp,

                    COALESCE(brightness = 0, FALSE) AS is_zero_brightness,

                    (  
                        acq_timestamp_utc IS NULL
                        OR strftime(
                            acq_timestamp_utc,
                            '%Y-%m-%d%H%M'
                        ) != (
                            CAST(acq_date AS VARCHAR)
                            || LPAD(TRIM(acq_time_raw), 4, '0')
                        )
                    ) AS has_timestamp_issue,

                    (
                        acq_date IS NULL
                        OR acq_timestamp_utc IS NULL
                        OR latitude IS NULL
                        OR longitude IS NULL
                        OR brightness IS NULL
                        OR bright_t31 IS NULL
                        OR frp IS NULL
                        OR scan IS NULL
                        OR track IS NULL
                    ) AS has_parse_issue,

                    (
                        satellite_platform IS NULL
                        OR collection_version IS NULL
                    ) AS has_standardization_issue

                FROM standardized
            )

            SELECT
                {sql_string(id_prefix)}
                    || '_'
                    || LPAD(CAST(source_record_number AS VARCHAR), 9, '0')
                    AS observation_id,

                source_group,
                source_file,
                source_record_number,
                ingested_at_utc,

                satellite_raw,
                satellite_platform,
                sensor_family,
                instrument_raw,

                version_raw,
                collection_version,

                acq_date,
                acq_time_raw,
                acq_timestamp_utc,

                latitude,
                longitude,

                brightness,
                bright_t31,
                frp,
                scan,
                track,

                daynight,
                confidence_raw,
                confidence_numeric,
                confidence_category,
                type_raw,

                is_negative_frp,
                is_zero_brightness,
                has_parse_issue,
                has_timestamp_issue,
                has_standardization_issue,

                (
                    is_negative_frp
                    OR is_zero_brightness
                    OR has_parse_issue
                    OR has_standardization_issue
                    OR has_timestamp_issue
                ) AS has_quality_issue

            FROM flagged

        )
        TO {temp_sql}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        print(f"Transforming: {input_file.name}")
        con.execute(query)

        # Verify that the output contains exactly as many records
        # as the input. No observations should be silently discarded.
        input_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(input_file)]
        ).fetchone()[0]

        output_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(temp_file)]
        ).fetchone()[0]

        if input_count != output_count:
            raise RuntimeError(
                f"Record count mismatch: Bronze={input_count:,}, "
                f"Silver={output_count:,}"
            )

        # Check the generated IDs.
        id_stats = con.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(observation_id) AS non_null_ids,
                COUNT(DISTINCT observation_id) AS unique_ids
            FROM read_parquet(?)
            """,
            [str(temp_file)]
        ).fetchone()

        if id_stats[0] != id_stats[1] or id_stats[0] != id_stats[2]:
            raise RuntimeError(
                "Observation ID validation failed: "
                f"total={id_stats[0]:,}, "
                f"non_null={id_stats[1]:,}, "
                f"unique={id_stats[2]:,}"
            )

        # Check the output schema.
        schema = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(temp_file)]
        ).fetchall()

        if len(schema) != 32:
            raise RuntimeError(
                f"Expected 32 Silver columns, found {len(schema)}"
            )

        # Publish only after all checks pass.
        temp_file.replace(output_file)

        print(f"Successfully transformed: {input_file.name}")
        print(f"Records: {output_count:,}")
        print(f"Columns: {len(schema)}")
        print(f"Output: {output_file}")

    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise

    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transform one Bronze Parquet file into Silver."
    )

    parser.add_argument(
        "--group",
        required=True,
        choices=["MODIS", "NOAA20", "NOAA21", "SNPP"],
        help="Source group of the input file.",
    )

    parser.add_argument(
        "--file",
        required=True,
        help="Bronze Parquet filename.",
    )

    args = parser.parse_args()

    input_file = DEFAULT_BRONZE_ROOT / args.group / args.file
    output_file = DEFAULT_SILVER_ROOT / args.group / args.file

    transform_file(input_file, output_file, args.group)


if __name__ == "__main__":
    main()
