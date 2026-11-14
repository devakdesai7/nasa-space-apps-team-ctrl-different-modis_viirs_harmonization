
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