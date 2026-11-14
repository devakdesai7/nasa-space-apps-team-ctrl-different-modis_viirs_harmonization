
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
