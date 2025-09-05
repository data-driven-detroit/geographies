"""
This script was used as a helper to create the field reference files found on conf.
Not required currently, but could be useful in the future when downloading new
datasets.
"""

import json
import os
from pathlib import Path
import geopandas as gpd
import tomllib


# Global configuration
with open("config.toml", "rb") as f:
    config = tomllib.load(f)

# This is a rough template for the field reference file
TO_APPEND = """

{
    "geoid_prefix": "",
    "geoid_suffix_col": "",
    "geo_type": "",
    "renames": {
    }
}"""


def main():
    for path in os.listdir(Path.home() / config["destination_dir"]):
        stem = Path(path).stem

        print(f"{stem}.json", end="\t") 

        with open(Path("conf") / "field_references" / f"{stem}.json", "r+") as f:
            content = f.read().strip()
            ref = json.loads(content) if content else []

            match ref:
                case dict():
                    print(f"Field reference for {path} already present.")
                    continue

                case _:
                    print(f"Field reference for {path} not found.")
                    frame = gpd.read_file(Path.home() / config["destination_dir"] / path)
                    json.dump(list(frame.columns), f)
                    f.write(TO_APPEND)


if __name__ == "__main__":
    main()
