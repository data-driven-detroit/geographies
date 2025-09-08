CREATE TABLE geographic_adjacency AS
SELECT 
    a.geoid AS center, 
    b.geoid AS surround,
    'tract' AS geo_type
FROM geographies a
JOIN geographies b
    ON st_touches(a.geometry, b.geometry)
WHERE
    a.geoid LIKE '140%' -- Wayne County
    AND b.geoid LIKE '140%'
    AND a.start_date = DATE '2020-01-01'
    AND b.start_date = DATE '2020-01-01'
ORDER BY a.geoid, b.geoid;
    
