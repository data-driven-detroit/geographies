CREATE TABLE etl_geographies (
    geoid VARCHAR(64),
    geo_type VARCHAR(64),
    name VARCHAR(512),
    aland NUMERIC,
    awater NUMERIC,
    start_date DATE,
    end_date DATE,
    geometry GEOMETRY,
);
