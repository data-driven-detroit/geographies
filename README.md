# Goals for SDC and HIP

- [ ] The geographies should be tightly specified and change-managed
    - The main strategy to accomplish this is to have a table that includes all
      geographies with start and end dates. That way we can have a picture of
      which geographies were valid at any given point in time. Mostly we're
      covering from 2010 onward, though for main-line census geographies we'll
      include back to 2000 as well.
    - [ ] TODO script to evaluate whether the currently active geography matches 
          the new incoming geography and therefore ignore the new.
    - [ ] Geography types:
        - state
            - Basically don't change so only one entry from 2000-present
        - county
            - Haven't changed since 1970s, and not majorly in at least 100 years
              so we'll have single entries from 2000-present
        - county subdivisions
            - TODO: Investigate
        - tracts
            - Keeping these at the 10-year marks, and not worrying about
              mid-decade adjustments for now
        - zctas
            - TODO: Investigate
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
            - Keeping these at the 10-year marks, and not worrying about
              mid-decade adjustments for now
- [ ] The updates should be as close to completely automated as possible
- [ ] The text on the main page should not require coding to make revisions
- [ ] These are all transformed to EPSG:2898
