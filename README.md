# WCS Raster Downloader

CLI tool for downloading DTM (Digital Terrain Model) rasters from Geonorge WCS service for polygon geometries.

## Features

- Parallel downloads with configurable worker count
- Progress bar with live status (completed/skipped/failed)
- Automatic resume - skips already downloaded files
- Error logging to CSV for failed polygons
- Auto-reprojection to EPSG:25833 if needed
- Input validation (NaN bounds detection, max raster size limits)
- WCS error response detection with clear error messages

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd laster_ned_raster_wcs_cc

# Install dependencies with uv
uv sync
```

## Usage

```bash
uv run python wcs_downloader.py INPUT_PARQUET OUTPUT_DIR [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `INPUT_PARQUET` | Path to parquet file containing polygon geometries |
| `OUTPUT_DIR` | Directory where output GeoTIFF files will be saved |

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--workers` | `-w` | 4 | Number of parallel download workers |
| `--sleep` | `-s` | 0.5 | Seconds to sleep between requests per worker |
| `--resolution` | `-r` | 1.0 | Output resolution in meters per pixel |
| `--max-pixels` | `-m` | 10000 | Maximum pixels per dimension |
| `--wcs-url` | | Geonorge DTM | WCS service URL |
| `--coverage-id` | | nhm_dtm_topo_25833 | Coverage identifier |

## Examples

### Basic usage

Download DTM rasters for all polygons in a parquet file:

```bash
uv run python wcs_downloader.py input_data/høymyr_nordland.parquet output/
```

### With custom workers and sleep time

Use 8 parallel workers with 0.3 second delay between requests:

```bash
uv run python wcs_downloader.py input_data/myr_nordland.parquet output/ --workers 8 --sleep 0.3
```

### Short form options

```bash
uv run python wcs_downloader.py input_data/høymyr_nordland.parquet output/ -w 4 -s 0.5
```

### Custom resolution

Download at 0.5m resolution (higher detail, larger files):

```bash
uv run python wcs_downloader.py input_data/høymyr_nordland.parquet output/ -r 0.5
```

Download at 2m resolution (lower detail, smaller files):

```bash
uv run python wcs_downloader.py input_data/høymyr_nordland.parquet output/ -r 2
```

### Resume interrupted download

Simply run the same command again. Existing files will be skipped automatically:

```bash
# First run - downloads 50 of 100 polygons before interruption
uv run python wcs_downloader.py data.parquet output/ -w 4

# Second run - skips the 50 already downloaded, continues with remaining 50
uv run python wcs_downloader.py data.parquet output/ -w 4
```

### Using a different WCS service

```bash
uv run python wcs_downloader.py input.parquet output/ \
    --wcs-url "https://wcs.example.com/service" \
    --coverage-id "coverage_name"
```

## Output

### Downloaded rasters

Output files are saved as GeoTIFF with the naming pattern `D_{resolution}m_{index}.tif`, where `resolution` is the configured resolution and `index` is the polygon's row index from the input parquet file.

```
output/
├── D_1.0m_410.tif
├── D_1.0m_413.tif
├── D_1.0m_416.tif
└── ...
```

### Error log

If any downloads fail, a CSV file `failed_polygons.csv` is created in the output directory:

| Column | Description |
|--------|-------------|
| index | Polygon index from input file |
| minx, miny, maxx, maxy | Bounding box coordinates |
| error_type | Exception type (e.g., ConnectionError) |
| error_message | Error details |
| timestamp | When the error occurred |

## Progress display

```
WCS Raster Downloader
========================================
Input:      input_data/høymyr_nordland.parquet
Output:     output
Workers:    4
Sleep:      0.5s
Resolution: 1.0m
Max pixels: 10000

Loading polygons from input_data/høymyr_nordland.parquet...
Found 99 polygons to process
  Downloading... ✓95 ⏭0 ✗4 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 99/99 0:00:22

       Download Summary
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ Status             ┃ Count ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ Completed          │    95 │
│ Skipped (existing) │     0 │
│ Failed             │     4 │
│ Total              │    99 │
└────────────────────┴───────┘
```

## Validation & Limits

The tool includes several safeguards to prevent errors and protect the WCS service:

### Maximum raster size

By default, each raster request is limited to **10,000 × 10,000 pixels** per dimension. This limit can be adjusted with the `--max-pixels` / `-m` option. For a polygon that would exceed this limit, the download will fail with a clear error message.

Example: A 15km × 15km polygon at 1m resolution would require 15,000 × 15,000 pixels. You can either:
- Use `-r 2` (2m resolution) to reduce to 7,500 × 7,500 pixels
- Use `-m 15000` to increase the limit to 15,000 × 15,000 pixels

### Invalid geometry detection

Polygons with invalid or empty geometries (resulting in NaN bounds) are detected and reported with clear error messages rather than causing cryptic failures.

### WCS error handling

If the WCS service returns an error (e.g., coverage not found, server error), the tool detects the XML error response and reports it clearly instead of failing with "not a valid GeoTIFF".

## Input requirements

- Input file must be a GeoParquet file (`.parquet`) with polygon geometries
- Coordinates can be in any CRS (auto-reprojected to EPSG:25833)
- Each polygon's bounding box is used to request DTM data from the WCS service
- Polygons must have valid geometries (no empty or null geometries)

## Dependencies

- typer
- rich
- geopandas
- geoutils
- owslib
- pyarrow
- pyproj
