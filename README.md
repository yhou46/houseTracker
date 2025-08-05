# Real estate history tracker

## TODO
- Add merge property history functionalities
- Convert spider json output to iproperty class
- Create DB schema, store data to DB
- Containerize the code, run with docker
- Deploy to cloud, run it daily
- Add parser test

## Goals
### Read
- Input: id, return full property info with history
- Input: address as string, return full property info with history
- Input: status: open, propertyType: singleFamily, price: below 1.5M, number_of_bed >= 3, zip_code: []
Return full property history

- Property Fields:
id: str,
addressHash -> string
area: PropertyArea -> number (convert to sqrt)
propertyType: PropertyType -> string
lotArea: PropertyArea | None,
numberOfBedrooms: float,
numberOfBathrooms: float,
yearBuilt: int | None,
status: PropertyStatus, -> string
price: float | None,
history: IPropertyHistory,
lastUpdated: datetime,
dataSource: List[IPropertyDataSource] = [],

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
    - pending time, like pending in past month, it is from history entry
    - sold time, it is from history entry
    - sold price, like find properties where sold price > list price

### Write
- Get data from multiple sources
    - Redfin
    - Zillow

- Maintain data integrity
    - No data can be changed.
    - Fix and mark mismatch in Redfin/Zillow price history. Sometimes these sites remove history like price change history.

## Store the data
- Amazon Dynamo DB schema
Partition key: property_id
Sort key: property-history-#: for property history event, older to newer history, like property-history-1, property-history-1
Each porperty history event should have attributes like date, description and type
Sort key property-metadata: for property metadata like number of bed room, area...

- SQL DB