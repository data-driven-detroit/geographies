import os
from pathlib import Path
import json
import asyncio
import tomllib
import geopandas as gpd
import pandas as pd
import click


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
        "curl", url, "-o", outpath,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await proc.wait()
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
    for path in os.listdir(config["destination_dir"]):
        stem = Path(path).stem
        try:
            frame = gpd.read_file(str(Path.cwd() / config["destination_dir"] / path))
            with open(Path("conf") / f"{stem}.json", "w") as f:
                json.dump(list(frame.columns), f)
        except:
            print(stem, "failed to open")


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
