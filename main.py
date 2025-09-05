"""
This file includes all steps of the ETL. You can skip them with flags when
running the script. This will download from census if you haven't already,
TODO transform to a uniform schema, and TODO load into your database.
"""

from pathlib import Path
import json
import asyncio
import tomllib
import geopandas as gpd
import pandas as pd
import click
from pyogrio.errors import DataSourceError
import pandera.pandas as pa
from pandera.typing.pandas import Series
from pandera.engines.geopandas_engine import Geometry
from sqlalchemy import create_engine
from datetime import date


class Geographies(pa.DataFrameModel):
    geoid: Series[str]
    geo_type: Series[str]
    name: Series[str]
    aland: Series[float]
    awater: Series[float]
    start_date: Series[date]
    end_date: Series[date]
    geometry: Series[Geometry]


OUTPUT_COLS = Geographies.to_schema().columns.keys()


# Global configuration
with open("config.toml", "rb") as f:
    config = tomllib.load(f)


def get_engine():
    user = config["db"]["user"]
    password = config["db"]["password"]
    host = config["db"]["host"]
    port = config["db"]["port"]
    db = config["db"]["database"]

    return create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}")



async def download_file(ds: pd.Series, re_extract):
    outpath = f"{config['destination_dir']}/{ds['filename']}"
    url = f"https://www2.census.gov/{ds['directory']}/{ds['filename']}"

    if Path(outpath).exists() and not re_extract:
        print(f"{ds['filename']} already downloaded, continuing")
        return ds['filename']
    
    print(f"Downloading {ds['filename']}")
    proc = await asyncio.create_subprocess_exec(
        "curl", "-H", 
        '"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"', 
        url, "-o", outpath, "-k",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await proc.wait()

    if proc.returncode != 0:
        stderr_output = await proc.stderr.read()
        print(f"ERROR downloading {ds['filename']}: {stderr_output.decode()}")
        return None  # or raise exception

    return ds['filename']


async def download_files(datasets: pd.DataFrame, re_extract, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def limited_download(ds):
        async with semaphore:
            return await download_file(ds, re_extract)

    jobs = [limited_download(ds) for _, ds in datasets.iterrows()]

    return await asyncio.gather(*jobs)


def extract(re_extract):
    print("Extracting shape files from census")
    
    assert Path(config["destination_dir"]).exists()

    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    asyncio.run(download_files(datasets, re_extract))


@pa.check_types()
def normalize_frame(frame, fref, start_date, end_date) -> Geographies:
    return (
        frame
        .rename(columns=fref["renames"])
        .assign(
            geoid=lambda frame: fref["geoid_prefix"] + frame[fref["geoid_suffix_col"]],
            geo_type=fref["geotype"],
            start_date=pd.to_datetime(start_date).dt.date,
            end_date=pd.to_datetime(end_date).dt.date,
        )[OUTPUT_COLS]
    )


def transform_and_load():
    db = get_engine()
    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    
    if_exists = "replace"
    for _, ds in datasets.iterrows():
        try:
            frame = gpd.read_file(Path(config["destination_dir"]) / ds["filename"])
            fref = json.loads((Path("conf") / "field_references" / ds["field_reference"]).read_text())
            normalized = normalize_frame(frame, fref, ds["start_date"], ds["end_date"])
            normalized_geo = gpd.GeoDataFrame(normalized)
            normalized_geo.to_postgis("geographies", db, if_exists=if_exists)

        except DataSourceError:
            print(f"Error reading {ds['filename']}")

        finally:
            if_exists = "append"


@click.command()
@click.option("--no-extract", "-e", is_flag=True, help="Skip extract step")
@click.option("--re-extract", "-r", is_flag=True, help="Download all files again")
@click.option("--no-transform-load", "-t", is_flag=True, help="Skip transform and load steps")
def main(no_extract, re_extract, no_transform_load):
    if not no_extract:
        extract(re_extract)

    if not no_transform_load:
        transform_and_load()


if __name__ == "__main__":
    main()

