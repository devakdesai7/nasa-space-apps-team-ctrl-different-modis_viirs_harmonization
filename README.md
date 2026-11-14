
# MODIS-VIIRS Fire Data Harmonization

A multi-satellite data engineering and harmonization pipeline
for MODIS and VIIRS active-fire observations.

## Satellite Platforms

- Terra MODIS
- Aqua MODIS
- Suomi-NPP VIIRS
- NOAA-20 VIIRS
- NOAA-21 VIIRS

## Architecture

The project follows a medallion architecture:

- Bronze: Raw satellite data ingestion
- Silver: Data cleaning and standardization
- Gold: Spatial-temporal harmonization and analytical datasets

## Technology Stack

- Python
- DuckDB
- Polars
- PyArrow
- Parquet

## Data Sources

NASA FIRMS: https://firms.modaps.eosdis.nasa.gov/

Raw datasets are not included in this repository.


## Data Profiling and Source Assessment

### 1. Dataset Overview

The project currently uses NASA FIRMS active-fire archive CSV data covering three annual periods:

- March 1, 2023 – February 29, 2024
- March 1, 2024 – February 28, 2025
- March 1, 2025 – February 28, 2026

The dataset consists of **12 CSV files**, representing five satellite platforms across two sensor families:

| Sensor family | Satellite platforms | Product collection |
|---|---|---|
| MODIS | Terra, Aqua | Collection 6.1 |
| VIIRS | Suomi-NPP, NOAA-20, NOAA-21 | Collection 2 |

The combined dataset contains approximately **196 million active-fire detection records**.

### 2. Source Schema

All 12 source files share the same 15-column schema:

`latitude`, `longitude`, `brightness`, `scan`, `track`, `acq_date`, `acq_time`, `satellite`, `instrument`, `confidence`, `version`, `bright_t31`, `frp`, `daynight`, `type`

The profiling process examined:

- Record counts and acquisition date ranges
- Satellite and instrument distributions
- Missing values in important fields
- Geographic coordinate validity
- Acquisition-time validity
- Numeric ranges and zero/negative values
- Confidence and detection-type distributions
- Exact duplicate records within individual source files

### 3. Profiling Results

The initial profiling and anomaly investigation established the following:

| Check | Result |
|---|---|
| Source CSV files | 12 |
| Common schema | 15 columns across all files |
| Exact duplicate rows | 0 within each individual file |
| Invalid acquisition times detected | 0 |
| Invalid latitude/longitude values detected | 0 |
| Non-positive scan/track values | 0 |
| Missing values in the initially checked fields | 0 |
| Negative FRP records | 1 |
| Zero-brightness records | 29 |

**Important:** Duplicate detection was performed separately for each CSV. Cross-file duplicate detection has not yet been performed.

### 4. Data Anomalies

#### 4.1 Negative Fire Radiative Power

One record in `fire_archive_M-C61_846655.csv` contains a negative FRP value:

| Attribute | Value |
|---|---|
| Acquisition date | 2025-08-22 |
| Acquisition time | 2126 UTC |
| Satellite | Aqua |
| Latitude | 43.3536 |
| Longitude | -109.9984 |
| FRP | -5.9 MW |

Negative FRP is not physically meaningful as a fire-power measurement. The record will be retained in the raw layer and flagged during Silver-layer quality assessment.

#### 4.2 Zero Brightness Values

A total of 29 records across the three SNPP VIIRS source files contain `brightness = 0`.

These records share the following characteristics:

- Confidence category: `l` (low)
- Detection type: `0`
- Day/night indicator: `D`
- Positive FRP values
- Nonzero `bright_t31` values

The zero brightness values are outside the expected physical range of the VIIRS I-4 brightness-temperature measurement. However, the cause has not been established.

These records will be retained and assigned a field-level quality flag in the Silver layer.

### 5. Important Source Differences

The profiling process identified differences that must be accounted for during harmonization.

**Confidence representation**

- MODIS confidence is numeric, ranging from 0 to 100 in the profiled files.
- VIIRS confidence is categorical, using `l`, `n`, and `h`.

These values must not be treated as directly interchangeable. A sensor-aware confidence interpretation will be developed in the transformation layer.

**Instrument labels**

The source `instrument` column contains:

- `MODIS` for MODIS products.
- `VIIRS` for NOAA-20 and NOAA-21 products.
- `SNPP` for Suomi-NPP products.

The original values will be preserved. A standardized sensor-family field will be introduced separately during Silver-layer transformation.

**Detection type**

All source products contain type codes 0, 1, 2, and 3. Their interpretation must be verified against the documentation for each respective product. The MODIS type-code definitions will not automatically be applied to VIIRS.

**Acquisition time**

The source `acq_time` field is represented as a four-digit HHMM value. It will be preserved in its original form in Bronze and converted into a standardized timestamp in Silver.

### 6. Engineering Decisions Derived from Profiling

The profiling results inform the following pipeline design decisions:

1. **Preserve source data:** Original CSV files remain unchanged and serve as the source of truth.
2. **Immutable Bronze layer:** Source values will be retained without cleaning or silently correcting anomalous records.
3. **Field-level quality flags:** Invalid measurements will be flagged individually rather than automatically removing entire detection records.
4. **Sensor-aware transformations:** MODIS and VIIRS measurements and confidence representations will retain their sensor-specific context.
5. **Provenance:** Source file, product, satellite platform, and collection/version information will be retained throughout the pipeline.
6. **Timestamp standardization:** Acquisition date and time will be converted into a standardized UTC timestamp in Silver.
7. **No unsupported assumptions:** Missing detections will not be interpreted as evidence of no fire, since these archives contain active-fire detections rather than complete observation/no-fire records.

### 7. Profiling Implementation

Profiling scripts are maintained under:

`src/profiling/`

- `profile_data.py` — Initial source-data profiling.
- `profile_data_stage2.py` — Detailed schema, distribution, and numeric-value profiling.
- `investigate_anomalies.py` — Extraction and inspection of anomalous records.

The profiling stage establishes the initial data-quality baseline for the Bronze → Silver → Gold data pipeline.


## Bronze Layer — Raw Data Ingestion

### 1. Objective

The Bronze layer is the first stage of the data pipeline. Its purpose is to convert the original NASA FIRMS active-fire CSV datasets into a columnar storage format (Parquet) while preserving the source data and its provenance.

The Bronze layer does not perform analytical transformations, sensor harmonization, data cleaning, or anomaly correction. Its primary responsibility is to establish a reliable, query-efficient representation of the source datasets for subsequent processing.

### 2. Input Data

The ingestion pipeline processes 12 CSV files covering five satellite platforms across three annual periods:

| Satellite platform | Sensor family | Number of files |
|---|---|---:|
| Terra and Aqua | MODIS | 3 |
| NOAA-20 | VIIRS | 3 |
| NOAA-21 | VIIRS | 3 |
| Suomi-NPP | VIIRS | 3 |
| **Total** | | **12** |

All source files share the same 15-column schema:

`latitude, longitude, brightness, scan, track, acq_date, acq_time, satellite, instrument, confidence, version, bright_t31, frp, daynight, type`

The source datasets contain approximately 14.8 GiB of CSV data and **196,335,668 records**.

### 3. Implementation

The ingestion pipeline is implemented in:

`src/ingestion/bronze_ingestion.py`

**Technology:** Python and DuckDB.

The pipeline performs the following operations:

1. Discovers CSV files inside the configured source-data directory.
2. Validates that each input file has the expected source schema.
3. Reads source columns as strings using DuckDB's CSV reader (`all_varchar=true`).
4. Adds ingestion metadata to each record.
5. Writes the resulting dataset to Parquet using Zstandard (ZSTD) compression.
6. Validates the output record count against the corresponding input file.
7. Uses a temporary output file before renaming it to its final name.

### 4. Data Preservation and Metadata

All 15 original source columns are retained without type conversion or analytical modification.

Three metadata columns are appended:

| Column | Description |
|---|---|
| `source_group` | Identifies the source group: MODIS, NOAA20, NOAA21, or SNPP. |
| `source_file` | Original CSV filename from which the record was ingested. |
| `ingested_at_utc` | UTC timestamp recorded for the ingestion of the corresponding source file. |

Original source columns are stored as `VARCHAR` in Bronze.

This is intentional: the Bronze layer preserves the original field representations, including values such as zero-padded acquisition times (`0001`) and sensor-specific confidence representations.

Sensor-specific values are not normalized or converted at this stage.

### 5. Output Structure

Each source CSV produces one corresponding Parquet file.

```text
data/
└── bronze/
    ├── MODIS/
    │   ├── fire_archive_M-C61_846646.parquet
    │   ├── fire_archive_M-C61_846650.parquet
    │   └── fire_archive_M-C61_846655.parquet
    ├── NOAA20/
    │   ├── fire_archive_J1V-C2_846647.parquet
    │   ├── fire_archive_J1V-C2_846651.parquet
    │   └── fire_archive_J1V-C2_846656.parquet
    ├── NOAA21/
    │   ├── fire_archive_J2V-C2_846648.parquet
    │   ├── fire_archive_J2V-C2_846652.parquet
    │   └── fire_archive_J2V-C2_846657.parquet
    └── SNPP/
        ├── fire_archive_SV-C2_846649.parquet
        ├── fire_archive_SV-C2_846653.parquet
        └── fire_archive_SV-C2_846658.parquet
```

The original CSV files and generated Parquet files are excluded from version control.

### 6. Ingestion Results

All 12 source files were successfully ingested.

| Source group | Period 1 | Period 2 | Period 3 | Total records |
|---|---:|---:|---:|---:|
| MODIS | 4,707,068 | 4,817,661 | 3,587,395 | 13,112,124 |
| NOAA-20 | 21,429,101 | 22,576,988 | 18,088,815 | 62,094,904 |
| NOAA-21 | 21,118,821 | 21,681,655 | 17,278,920 | 60,079,396 |
| Suomi-NPP | 21,969,453 | 20,970,878 | 18,108,913 | 61,049,244 |
| **Total** | **69,224,443** | **70,047,182** | **57,064,043** | **196,335,668** |

For every source file, the input CSV record count matched the generated Parquet record count.

### 7. Idempotency and Re-ingestion

The ingestion script skips output files that already exist by default. This prevents unnecessary reprocessing when the pipeline is executed again.

To explicitly rebuild existing Bronze outputs, use the `--overwrite` option.

The pipeline writes to a temporary Parquet file before renaming it to the final output filename. This reduces the risk of leaving a partially written file under the final output name if ingestion fails.

### 8. Bronze Validation

Validation is implemented separately in:

`src/ingestion/validate_bronze.py`

The validation script checks:

- Exactly 12 expected Parquet files are present.
- The output schema contains the 15 original columns and 3 metadata columns.
- Original source columns retain the `VARCHAR` data type.
- `ingested_at_utc` has the `TIMESTAMP WITH TIME ZONE` data type.
- Source group and source filename metadata are consistent.
- The known negative FRP anomaly is preserved.
- All 29 known zero-brightness anomalies are preserved.

**Validation result:**

- 12/12 Parquet files passed validation.
- Total records: 196,335,668.
- Negative FRP anomaly preserved: 1.
- Zero-brightness anomalies preserved: 29.
- All Bronze validation checks passed.

These checks establish structural consistency, record-count agreement, metadata consistency, and preservation of selected known anomalies. They do not constitute a complete row-by-row comparison or a byte-for-byte equivalence test against the original CSV files.

### 9. Engineering Decisions

**Why Parquet?**

Parquet is a columnar storage format. Unlike CSV, it stores data by column, enabling analytical query engines such as DuckDB to read only the columns required by a query. It also supports compression and typed metadata, making it suitable for large analytical datasets.

**Why DuckDB?**

DuckDB provides an embedded analytical SQL engine capable of processing large CSV and Parquet datasets without requiring a distributed computing cluster. It is sufficient for the current dataset scale and keeps the development environment lightweight.

**Why retain all source columns as strings?**

The Bronze layer is intended to preserve source representations rather than impose analytical interpretations. Type conversion, timestamp construction, standardized satellite identifiers, and sensor-specific field handling are deferred to the Silver layer.

**Why retain anomalies?**

Anomalous records are not automatically deleted or corrected during ingestion. Their presence is documented through profiling, and they remain available for subsequent quality assessment and analysis.

**Why one Parquet file per source CSV?**

This preserves a direct relationship between each source file and its ingested output. Partitioning and file compaction can be considered later if query performance or file-management requirements justify them.

### 10. Running the Bronze Pipeline

Activate the project's virtual environment and execute:

```powershell
python src/ingestion/bronze_ingestion.py
```

To rebuild existing outputs:

```powershell
python src/ingestion/bronze_ingestion.py --overwrite
```

Run validation separately:

```powershell
python src/ingestion/validate_bronze.py
```

