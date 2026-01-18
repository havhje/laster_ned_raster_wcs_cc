#!/usr/bin/env python3
"""
WCS Raster Downloader CLI Tool

Downloads DTM rasters from Geonorge WCS service for polygons in a parquet file.
Supports parallel processing, error logging, and resume capability.
"""

import csv
import math
import tempfile
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated

# Default maximum pixels per dimension to prevent huge requests
DEFAULT_MAX_PIXELS = 10000

# Suppress geoutils deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="geoutils")
warnings.filterwarnings("ignore", message="No nodata set")

import geopandas as gpd
import geoutils as gu
import typer
from pyproj import CRS
from owslib.wcs import WebCoverageService
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

# Thread-local storage for WCS connections (one per thread)
thread_local = threading.local()

console = Console()


@dataclass
class DownloadResult:
    """Result of a single polygon download attempt."""

    index: int
    success: bool
    skipped: bool = False
    error_type: str | None = None
    error_message: str | None = None


@dataclass
class FailedPolygon:
    """Details of a failed polygon download for error logging."""

    index: int
    minx: float
    miny: float
    maxx: float
    maxy: float
    error_type: str
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


def get_wcs_connection(wcs_url: str) -> WebCoverageService:
    """Get or create a thread-local WCS connection."""
    if not hasattr(thread_local, "wcs"):
        thread_local.wcs = WebCoverageService(wcs_url, version="1.0.0")
    return thread_local.wcs


def download_single_polygon(
    index: int,
    row,
    polygon_gdf: gpd.GeoDataFrame,
    wcs_url: str,
    coverage_id: str,
    output_folder: Path,
    sleep_duration: float,
    resolution: float,
    max_pixels: int,
) -> DownloadResult:
    """
    Download and mask raster for a single polygon.

    Args:
        index: Polygon index from the GeoDataFrame
        row: Row from bounds DataFrame with minx, miny, maxx, maxy
        polygon_gdf: Original GeoDataFrame with polygon geometries
        wcs_url: WCS service URL
        coverage_id: Coverage identifier
        output_folder: Directory for output files
        sleep_duration: Seconds to sleep after request
        resolution: Output resolution in meters per pixel
        max_pixels: Maximum pixels per dimension

    Returns:
        DownloadResult with success status and any error details
    """
    output_file = output_folder / f"D_{resolution}m_{index}.tif"

    # Skip if file already exists
    if output_file.exists():
        return DownloadResult(index=index, success=True, skipped=True)

    try:
        # Validate bounds are not NaN (can happen with invalid geometries)
        bounds = [row.minx, row.miny, row.maxx, row.maxy]
        if any(math.isnan(v) for v in bounds):
            raise ValueError(f"Invalid geometry bounds (NaN values) for polygon {index}")

        wcs = get_wcs_connection(wcs_url)

        # Create bbox tuple for WCS 1.0.0
        bbox = (
            float(row.minx),
            float(row.miny),
            float(row.maxx),
            float(row.maxy),
        )

        # Calculate pixel dimensions based on resolution
        width = max(1, int((row.maxx - row.minx) / resolution))
        height = max(1, int((row.maxy - row.miny) / resolution))

        # Prevent excessively large raster requests
        if width > max_pixels or height > max_pixels:
            raise ValueError(
                f"Requested raster too large: {width}x{height} pixels. "
                f"Max is {max_pixels}x{max_pixels}. "
                f"Consider using a coarser resolution or increasing --max-pixels."
            )

        # Request coverage from WCS
        response = wcs.getCoverage(
            identifier=coverage_id,
            bbox=bbox,
            crs="EPSG:25833",
            format="GeoTIFF",
            width=width,
            height=height,
        )

        downloaded_data = response.read()

        # Check for WCS error response (XML error documents start with '<')
        if downloaded_data.startswith(b'<') or downloaded_data.startswith(b'<?'):
            error_preview = downloaded_data[:500].decode('utf-8', errors='replace')
            raise ValueError(f"WCS returned error response: {error_preview}")

        # Write to temp file, process, and clean up
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            tmp.write(downloaded_data)
            temp_path = Path(tmp.name)

        try:
            raster = gu.Raster(temp_path, load_data=True)

            # Create mask from polygon geometry
            vector = gu.Vector(polygon_gdf.loc[[index]])
            mask = vector.create_mask(ref=raster)

            # Apply mask: ~mask inverts so outside polygon becomes NoData
            raster.set_mask(~mask)

            # Save output
            raster.save(output_file)

        finally:
            # Clean up temp file
            temp_path.unlink(missing_ok=True)

        # Sleep to avoid overwhelming the server
        if sleep_duration > 0:
            time.sleep(sleep_duration)

        return DownloadResult(index=index, success=True)

    except Exception as e:
        return DownloadResult(
            index=index,
            success=False,
            error_type=type(e).__name__,
            error_message=str(e),
        )


def write_error_log(failed: list[FailedPolygon], output_folder: Path) -> Path:
    """Write failed polygons to CSV file."""
    error_log_path = output_folder / "failed_polygons.csv"

    with open(error_log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["index", "minx", "miny", "maxx", "maxy", "error_type", "error_message", "timestamp"]
        )
        for fp in failed:
            writer.writerow(
                [fp.index, fp.minx, fp.miny, fp.maxx, fp.maxy, fp.error_type, fp.error_message, fp.timestamp]
            )

    return error_log_path


def process_polygons(
    input_parquet: Path,
    output_dir: Path,
    workers: int,
    sleep_duration: float,
    wcs_url: str,
    coverage_id: str,
    resolution: float,
    max_pixels: int,
) -> tuple[int, int, int, list[FailedPolygon]]:
    """
    Process all polygons with parallel downloads.

    Returns:
        Tuple of (completed_count, skipped_count, failed_count, failed_list)
    """
    # Load polygon data
    console.print(f"[blue]Loading polygons from {input_parquet}...[/blue]")
    polygon_gdf = gpd.read_parquet(input_parquet)

    # Reproject to EPSG:25833 if needed (WCS expects UTM coordinates)
    target_crs = CRS.from_epsg(25833)
    if polygon_gdf.crs is None:
        console.print("[yellow]Warning: No CRS found, assuming EPSG:25833[/yellow]")
        polygon_gdf = polygon_gdf.set_crs("EPSG:25833")
    elif not polygon_gdf.crs.equals(target_crs):
        console.print(f"[yellow]Reprojecting from {polygon_gdf.crs} to EPSG:25833...[/yellow]")
        polygon_gdf = polygon_gdf.to_crs("EPSG:25833")

    bounds_df = polygon_gdf.bounds

    total = len(bounds_df)
    console.print(f"[green]Found {total} polygons to process[/green]")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    completed = 0
    skipped = 0
    failed_count = 0
    failed_polygons: list[FailedPolygon] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Downloading rasters...", total=total)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    download_single_polygon,
                    row.Index,
                    row,
                    polygon_gdf,
                    wcs_url,
                    coverage_id,
                    output_dir,
                    sleep_duration,
                    resolution,
                    max_pixels,
                ): row
                for row in bounds_df.itertuples()
            }

            # Process results as they complete
            for future in as_completed(futures):
                row = futures[future]
                result = future.result()

                if result.skipped:
                    skipped += 1
                elif result.success:
                    completed += 1
                else:
                    failed_count += 1
                    failed_polygons.append(
                        FailedPolygon(
                            index=result.index,
                            minx=row.minx,
                            miny=row.miny,
                            maxx=row.maxx,
                            maxy=row.maxy,
                            error_type=result.error_type or "Unknown",
                            error_message=result.error_message or "Unknown error",
                        )
                    )

                progress.update(task, advance=1)
                progress.update(
                    task,
                    description=f"[cyan]Downloading... [green]✓{completed}[/green] [yellow]⏭{skipped}[/yellow] [red]✗{failed_count}[/red]",
                )

    return completed, skipped, failed_count, failed_polygons


def main(
    input_parquet: Annotated[Path, typer.Argument(help="Path to parquet file with polygon geometries")],
    output_dir: Annotated[Path, typer.Argument(help="Directory for output GeoTIFF files")],
    workers: Annotated[int, typer.Option("--workers", "-w", help="Number of parallel workers")] = 4,
    sleep: Annotated[float, typer.Option("--sleep", "-s", help="Seconds to sleep between requests per worker")] = 0.5,
    resolution: Annotated[float, typer.Option("--resolution", "-r", help="Output resolution in meters per pixel")] = 1.0,
    max_pixels: Annotated[int, typer.Option("--max-pixels", "-m", help="Maximum pixels per dimension")] = DEFAULT_MAX_PIXELS,
    wcs_url: Annotated[str, typer.Option("--wcs-url", help="WCS service URL")] = "https://wcs.geonorge.no/skwms1/wcs.hoyde-dtm-nhm-25833",
    coverage_id: Annotated[str, typer.Option("--coverage-id", help="Coverage identifier")] = "nhm_dtm_topo_25833",
) -> None:
    """
    Download DTM rasters from Geonorge WCS for polygons in a parquet file.

    Supports parallel processing, automatic resume (skips existing files),
    and logs failed polygons to a CSV file.
    """
    console.print()
    console.print("[bold blue]WCS Raster Downloader[/bold blue]")
    console.print("=" * 40)
    console.print(f"Input:      {input_parquet}")
    console.print(f"Output:     {output_dir}")
    console.print(f"Workers:    {workers}")
    console.print(f"Sleep:      {sleep}s")
    console.print(f"Resolution: {resolution}m")
    console.print(f"Max pixels: {max_pixels}")
    console.print()

    if not input_parquet.exists():
        console.print(f"[red]Error: Input file not found: {input_parquet}[/red]")
        raise typer.Exit(1)

    # Process all polygons
    completed, skipped, failed_count, failed_polygons = process_polygons(
        input_parquet=input_parquet,
        output_dir=output_dir,
        workers=workers,
        sleep_duration=sleep,
        wcs_url=wcs_url,
        coverage_id=coverage_id,
        resolution=resolution,
        max_pixels=max_pixels,
    )

    # Print summary
    console.print()
    table = Table(title="Download Summary")
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")
    table.add_row("[green]Completed[/green]", str(completed))
    table.add_row("[yellow]Skipped (existing)[/yellow]", str(skipped))
    table.add_row("[red]Failed[/red]", str(failed_count))
    table.add_row("[bold]Total[/bold]", str(completed + skipped + failed_count))
    console.print(table)

    # Write error log if there were failures
    if failed_polygons:
        error_log_path = write_error_log(failed_polygons, output_dir)
        console.print()
        console.print(f"[red]Failed polygons logged to: {error_log_path}[/red]")

        # Show first few failures
        if len(failed_polygons) <= 5:
            console.print()
            error_table = Table(title="Failed Polygons")
            error_table.add_column("Index")
            error_table.add_column("Error Type")
            error_table.add_column("Message")
            for fp in failed_polygons:
                error_table.add_row(str(fp.index), fp.error_type, fp.error_message[:50])
            console.print(error_table)

    console.print()


def cli() -> None:
    """Entry point for the CLI."""
    typer.run(main)


if __name__ == "__main__":
    cli()
