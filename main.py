from pathlib import Path
import asyncio
import tomllib
import geopandas as gpd
import pandas as pd
import click
from pyogrio.errors import DataSourceError


# Global configuration
with open("config.toml", "rb") as f:
    config = tomllib.load(f)


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
    
    # Make sure where we put the files exists
    assert Path(config["destination_dir"]).exists()

    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    asyncio.run(download_files(datasets, re_extract))


def transform():
    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    for _, ds in datasets.iterrows():
        try:
            frame = gpd.read_file(Path(config["destination_dir"]) / Path(ds["filename"]))

        except DataSourceError:
            print(f"Error reading {ds['filename']}")

# https://www2.census.gov/geo/tiger/TIGER2011/UNSD/tl_2011_26_unsd.zip

def load():
    pass


@click.command()
@click.option("--no-extract", "-e", is_flag=True, help="Skip extract step")
@click.option("--re-extract", "-r", is_flag=True, help="Download all files again")
@click.option("--no-transform", "-t", is_flag=True, help="Skip transform step")
@click.option("--no-load", "-l", is_flag=True, help="Skip load step")
def main(no_extract, re_extract, no_transform, no_load):
    if not no_extract:
        extract(re_extract)

    if not no_transform:
        transform()

    if not no_load:
        load()


if __name__ == "__main__":
    main()
