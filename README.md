# D3 Census Geographies

## Goals

- [ ] We only support a subset of the available census geographies,
      though we can add others in the future.
- [ ] The geographies should all live on a single table with geography type, 
      start date and end date as fields.
- [ ] Changes to geographies should be carefully tracked
- [ ] Final projections will be EPSG:2898 (transform for northern
      Michigan work if necessary)
- [ ] TODO: We need a script script to evaluate whether the currently active 
      geography matches the new incoming geography to avoid duplicating 
      geographies. End dates should be updated as well.

## Setup

This package uses uv for dependency management. If you have it installed you 
can run `uv sync` to install all dependencies.

## config.toml

The config.toml can live at the root of the project and needs the
directory where you'd like to download the shapefiles, and your
database credentials.

```toml
destination_dir="/where/you/want/to/download/files"

[db]
user="username"
password="pass"
host="hostname"
port="5432"
database="databasename"
```


## Currently supported geography types

- state
    - MI hasn't changed since the 1970s and that was a
      lake-boundary change.
- county
    - Haven't changed since 1970s, and not majorly in at least 100 years
      so we'll have single entries from 2000-present
- county subdivisions
    - Currently keeping only the most recent -- TODO: more research is
      needed to understand how these change.
- tracts
    - Keeping these at the 10-year marks, and not worrying about
      mid-decade adjustments for now
- zctas
    - Change with decennial censuses
- congressional district
    - Change with the decennial census, but we have an extra change in
      2023 due to a court challenge
- state senate district
    - Change with the decennial census, but we have an extra change in
      2023 due to a court challenge
- state house district
    - Change with the decennial census, but we have an extra change in
      2023 due to a court challenge
- school district 
    - These change almost yearly, though have calmed in recent years
    - District types:
        - unified *this is almost all of them*
        - elementary
        - secondary
- blocks *for crosswalks and other ETL use*
    - Updates after decennial censuses


## TODOs

- [ ] I grab the full-US file for congressional districts before
  the latest year where the census bureau broke them into
  separate files. I think I'll grab all of them, not that we do
  much political analysis, but they aren't many and it's useful
  for comparisons.
