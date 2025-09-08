"""
This file includes all steps of the ETL. You can skip them with flags when
running the script. This will download from census if you haven't already,
TODO transform to a uniform schema, and TODO load into your database.
"""

from pathlib import Path
import json
import datetime
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


class Geographies(pa.DataFrameModel):
    geoid: Series[str]
    geo_type: Series[str]
    name: Series[str]
    aland: Series[float]
    awater: Series[float]
    start_date: Series[datetime.date]
    end_date: Series[datetime.date]
    geometry: Series[Geometry]


OUTPUT_GEO_COLS = Geographies.to_schema().columns.keys()


class GeographicRelationships(pa.DataFrameModel):
    """
    In most cases, this will be earlier census geographies as a and 
    later as b. TODO (Mike): Revisit if this should be symmetrical so 
    user doesn't have to recall which is which.
    
    The goal is to allow users to easily translate from geography A 
    (typically older) to geo B:

    SELECT
        geoid_sink AS geoid, 
        SUM("Val from source geography" * weight) AS outcome_value
    FROM source_value_table vt -- This table is in geoid_a
    JOIN geographic_relationships gr
        ON gr.geoid_source = vt.geoid
    GROUP BY geoid_sink;

    This ETL is based on census relationship files, but there is nothing 
    stopping you from building similar files for any paris of geographies
    and governing any differences in field names with the 
    field_reference.json.
    """

    geoid_source: Series[str]
    geo_type_source: Series[str]
    start_date_source: Series[datetime.date]
    end_date_source: Series[datetime.date]
    geoid_sink: Series[str]
    geo_type_sink: Series[str]
    start_date_sink: Series[datetime.date]
    end_date_sink: Series[datetime.date]

    # These two can be null for custom relationships
    aland_part: Series[float] = pa.Field(nullable=True)
    awater_part: Series[float] = pa.Field(nullable=True)

    # These are required, otherwise there is no relationship
    weight: Series[float] = pa.Field(nullable=False)
    # This is if you neet to go sink -> source
    rweight: Series[float] = pa.Field(nullable=False)


OUTPUT_REL_COLS = GeographicRelationships.to_schema().columns.keys()


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
    assert Path(config["destination_dir"]).exists()

    print("Extracting shape files from census")
    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    asyncio.run(download_files(datasets, re_extract))
    
    print("Extracting relationship files")
    datasets = pd.read_csv("conf/relationship_files.csv")
    asyncio.run(download_files(datasets, re_extract))


@pa.check_types()
def normalize_geo_frame(frame, fref, start_date, end_date) -> Geographies:
    normalized = (
        frame
        .rename(columns=fref["renames"])
        .assign(
            geoid=lambda frame: fref["geoid_prefix"] + frame[fref["geoid_suffix_col"]],
            geo_type=fref["geo_type"],
            start_date=datetime.datetime.fromisoformat(start_date).date(),
            end_date=datetime.datetime.fromisoformat(end_date).date(),
        )
    )

    if fref["geo_type"] == "zcta":
        # Remove non-michigan ZCTAs to avoid huge files
        normalized = normalized[
            normalized["name"].str.startswith("48")
            | normalized["name"].str.startswith("49")
        ]

    return normalized[OUTPUT_GEO_COLS]


@pa.check_types()
def normalize_ref_frame(
    frame, fref, start_date_source, end_date_source, start_date_sink, 
    end_date_sink, weight_recipe="default"
) -> GeographicRelationships:
    
    if weight_recipe == "default":
        calc_weight =lambda frame: frame["aland_part"] / frame["__aland_source"]
        calc_rweight =lambda frame: frame["aland_part"] / frame["__aland_sink"]
    else:
        raise ValueError(f"Only the default weight calculation is supported.")

    normalized = (
        frame
        .rename(columns=fref["renames"])
        .assign(
            geoid_source=lambda frame: fref["geoid_source_prefix"] + frame[fref["geoid_source_suffix_col"]],
            geo_type_source=fref["geo_type_source"],
            start_date_source=start_date_source,
            end_date_source=end_date_source,
            geoid_sink=lambda frame: fref["geoid_sink_prefix"] + frame[fref["geoid_sink_suffix_col"]],
            geo_type_sink=fref["geo_type_sink"],
            start_date_sink=start_date_sink,
            end_date_sink=end_date_sink,
            weight=calc_weight,
            rweight=calc_rweight,
        )
    )

    return normalized[OUTPUT_REL_COLS]


def transform_and_load():
    db = get_engine()
    datasets = pd.read_csv("conf/tiger_mi_sources.csv")
    
    # First load the geographies
#     if_exists = "replace"
#     for _, ds in datasets.iterrows():
#         print(f"validating and loading {ds['filename']}")
#         try:
#             frame = gpd.read_file(Path(config["destination_dir"]) / ds["filename"])
#             fref = json.loads((Path("conf") / "field_references" / ds["field_reference"]).read_text())
#             normalized = normalize_geo_frame(frame, fref, ds["start_date"], ds["end_date"])
#             normalized_geo = gpd.GeoDataFrame(normalized)
#             normalized_geo.to_postgis("geographies", db, if_exists=if_exists)
# 
#         except DataSourceError:
#             print(f"Error reading {ds['filename']}")
# 
#         finally:
#             if_exists = "append"


    # Then load the relationship files
    relationship_files = pd.read_csv("conf/relationship_files.csv")
    if_exists = "replace"
    for _, ds in relationship_files.iterrows():
        print(f"validating and loading {ds['filename']}")
        try:
            fref = json.loads((Path("conf") / "field_references" / ds["field_reference"]).read_text())
            frame = pd.read_csv(
                Path(config["destination_dir"]) / ds["filename"], 
                delimiter=fref["delimiter"],
                dtype=fref["in_types"],
            )
            normalized = normalize_ref_frame(frame, fref, ds["start_date_source"], 
                ds["end_date_source"], ds["start_date_sink"], ds["end_date_sink"])
            normalized.to_sql("geographic_relationships", db, if_exists=if_exists)

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

