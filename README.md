# Real estate history tracker

## Goals
### Read
- Search by different criterias
    - property type
    - number of bedroom
    - number of bathroom
    - cities
    - zipcodes
    - area
    - lot area
    - list price
    - Price change history, like what properties have decrease in list price
    - list time, like newly listed
    - pending time, like pending in past month
    - sold time
    - sold price, like find properties where sold price > list price

### Write
- Get data from multiple sources
    - Redfin
    - Zillow

- Maintain data integrity
    - No data can be changed.
    - Fix and mark mismatch in Redfin/Zillow price history. Sometimes these sites remove history like price change history.
