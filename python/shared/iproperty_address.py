from enum import Enum
from typing import List, Dict

import logging

import usaddress # type: ignore

# USPS standard abbreviations for street suffixes and directionals
suffix_abbr = {
    "street": "St",
    "st": "St",
    "avenue": "Ave",
    "ave": "Ave",
    "boulevard": "Blvd",
    "blvd": "Blvd",
    "road": "Rd",
    "rd": "Rd",
    "drive": "Dr",
    "dr": "Dr",
    "lane": "Ln",
    "ln": "Ln",
    "court": "Ct",
    "ct": "Ct",
    "place": "Pl",
    "pl": "Pl",
    "terrace": "Ter",
    "ter": "Ter",
    "circle": "Cir",
    "cir": "Cir",
    "parkway": "Pkwy",
    "pkwy": "Pkwy",
    "way": "Way",
    "trail": "Trl",
    "trl": "Trl",
    "highway": "Hwy",
    "hwy": "Hwy",
    "driveway": "Dr",
    "drwy": "Dr",
    # Add more as needed
}
directional_abbr = {
    "north": "N",
    "n": "N",
    "south": "S",
    "s": "S",
    "east": "E",
    "e": "E",
    "west": "W",
    "w": "W",
    "northeast": "NE",
    "ne": "NE",
    "northwest": "NW",
    "nw": "NW",
    "southeast": "SE",
    "se": "SE",
    "southwest": "SW",
    "sw": "SW",
}

unit_abbr = {
    "apartment": "APT",
    "apt": "APT",
    "unit": "APT", # Normalize "unit" to "APT"
    "suite": "STE",
    "ste": "STE",
}

def _abbreviate_word(word: str, abbr_map: dict) -> str:
    return abbr_map.get(word.lower(), word)

def get_street_address(address: str, logger: logging.Logger | None = None) -> str:
    return get_address_components(address, logger)["street"]

def get_unit_information(address: str, logger: logging.Logger | None = None) -> str:
    return get_address_components(address, logger).get("unit", "")

def get_address_components(address: str, logger: logging.Logger | None = None) -> Dict[str, str]:
    try:
        components: Dict[str, str] = {}
        # Parse address string
        parsedAddress = usaddress.tag(address)
        addressPropertyBag = parsedAddress[0]

        addressType: str = parsedAddress[1]
        if (addressType != AddressType.StreetAddress.value):
            raise ValueError(f"Invalid address type: {addressType} for address: {address}")

        # Extract components
        # Street address
        # Build street part with abbreviation normalization
        street_components = [
            addressPropertyBag.get("AddressNumber", ""),
            addressPropertyBag.get("AddressNumberPrefix", ""),
            _abbreviate_word(addressPropertyBag.get("StreetNamePreDirectional", ""), directional_abbr),
            addressPropertyBag.get("StreetNamePreType", ""),
            addressPropertyBag.get("StreetName", ""),
            _abbreviate_word(addressPropertyBag.get("StreetNamePostType", ""), suffix_abbr),
            _abbreviate_word(addressPropertyBag.get("StreetNamePostDirectional", ""), directional_abbr)
        ]

        street = " ".join(filter(None, street_components))
        components["street"] = street

        # Unit information
        # Build unit part
        unit_components = [
            _abbreviate_word(addressPropertyBag.get("OccupancyType", ""), unit_abbr),
            addressPropertyBag.get("OccupancyIdentifier", "")
        ]
        
        # Handle # and Apt/Unit variations, # 116 -> APT 116
        if unit_components[0] == "" and unit_components[1].find("#") != -1:
            unit_components[0] = "APT"
            unit_components[1] = unit_components[1].replace("#", "").strip()
        unit = " ".join(filter(None, unit_components))
        if unit:
            components["unit"] = unit

        # City, state, zip
        city = addressPropertyBag.get("PlaceName", "")
        components["city"] = city

        state = addressPropertyBag.get("StateName", "")
        components["state"] = state

        zipcode = addressPropertyBag.get("ZipCode", "")
        components["zipcode"] = zipcode
        return components
    except Exception as e:
        # Fallback: normalize the original string if parsing fails
        error_msg = f"Error parsing address: {address}, error: {e}"
        if logger != None:
            logger.error(error_msg)
        else:
            print(error_msg)
        raise Exception(error_msg)

# Convert address string to a hash string
def get_address_hash(address: str, logger: logging.Logger | None = None) -> str:

    try:
        components = get_address_components(address, logger)
        ordered_keys = ["street", "unit", "city", "state", "zipcode"]
        ordered_components = [components[key] for key in ordered_keys if key in components]

        normalized = ",".join(ordered_components)
        # Apply normalization: lowercase, spaces to '-', commas to '|'
        normalized = normalized.lower().replace(" ", "-").replace(",", "|")
        return normalized
    except Exception as e:
        # Fallback: normalize the original string if parsing fails
        error_msg = f"Error parsing address: {address}, error: {e}"
        if logger != None:
            logger.error(error_msg)
        else:
            print(error_msg)
        raise Exception(error_msg)

class AddressType(Enum):
    StreetAddress = "Street Address"
    Intersection = "Intersection"
    POBox = "PO Box"
    Ambiguous = "Ambiguous"

# TODO: Use USPS address format API?
class IPropertyAddress:
    def __init__(self, address: str, logger: logging.Logger | None = None):
        components = get_address_components(address, logger)

        if components.get("street") is None or components.get("street") == "":
            raise ValueError(f"Invalid address: {address}. Street address is required.")
        self._streetName: str = components["street"]
        self._unit: str = components.get("unit", "")

        if components.get("city") is None or components.get("city") == "":
            raise ValueError(f"Invalid address: {address}. City is required.")
        self._city: str = components["city"]

        if components.get("state") is None or components.get("state") == "":
            raise ValueError(f"Invalid address: {address}. State is required.")
        self._state: str = components["state"]

        if components.get("zipcode") is None or components.get("zipcode") == "":
            raise ValueError(f"Invalid address: {address}. Zip code is required.")
        self._zipCode: str = components["zipcode"]

        self._addressHash: str = get_address_hash(address, logger)


    @property
    def streetName(self) -> str:
        return self._streetName

    @property
    def unit(self) -> str:
        return self._unit

    @property
    def state(self) -> str:
        return self._state

    @property
    def city(self) -> str:
        return self._city

    @property
    def zipCode(self) -> str:
        return self._zipCode

    def get_address_hash(self) -> str:
        return self._addressHash

    # This is index related
    def __eq__(self, other):
        if not isinstance(other, IPropertyAddress):
            return NotImplemented
        return self._addressHash == other._addressHash

    def __str__(self):
        return f"AddressHash: {self._addressHash}, Street: {self.streetName}, UnitNumber(if any): {self._unit}, State: {self._state}, ZipCode: {self._zipCode}"

if __name__ == "__main__":
    addressStr = "6910 Old Redmond Road unit 116, Redmond, WA 98052"
    print(f"Input: {addressStr}, Hashed: {get_address_hash(addressStr)}")