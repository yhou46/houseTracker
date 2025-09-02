from enum import Enum
from typing import List, Dict
import logging

import usaddress # type: ignore[import-untyped]

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

def _abbreviate_word(word: str, abbr_map: Dict[str, str]) -> str:
    return abbr_map.get(word.lower(), word)

def get_street_address(address: str, logger: logging.Logger | None = None) -> str:
    return get_address_components(address, logger)["street"]

def get_unit_information(address: str, logger: logging.Logger | None = None) -> str:
    return get_address_components(address, logger).get("unit", "")

class InvalidAddressError(Exception):
    def __init__(self, message: str, address: str):
        super().__init__(message)
        self.address = address

    def __str__(self) -> str:
        return super().__str__() + f"Address: {self.address})"

def preprocess_address_str(address_str: str) -> str:
    """
    Preprocess address string to handle special cases and normalize formatting.

    Args:
        address_str: Raw address string

    Returns:
        Preprocessed address string
    """
    # Handle cases like: 11170 (HS 24) NE 134th Ct NE, Redmond, WA 98052
    # Make it to be 11170 NE 134th Ct NE (HomeSite 24), Redmond, WA 98052
    start_idx = 0
    # Find opening and close parenthesis
    open_idx = address_str.find("(", start_idx)
    close_idx = address_str.find(")", open_idx)
    if open_idx != -1 and close_idx != -1 and open_idx < close_idx:

        # Extract content between parentheses
        content = address_str[open_idx + 1:close_idx].strip()

        # Check if content starts with "HS"
        if content.startswith("HS"):
            # Find the comma that separates street address from city/state/zip
            comma_idx = address_str.find(",", close_idx)
            if comma_idx != -1 and close_idx < comma_idx:
                # Extract the street address part (from start to comma)
                parts_before_parenthesis = address_str[:open_idx].strip()
                parts_after_parenthesis = address_str[close_idx+1 : comma_idx]

                # Extract the city/state/zip part (from comma onwards)
                other_part = address_str[comma_idx:]

                # Remove the (HS ...) from the beginning and add it to the end of street
                homesite_part = f"({content})"  # Keep original HS, don't convert to HomeSite yet

                print(f"street part: {parts_before_parenthesis}, other_part: {other_part}, content: {content}")

                # Reconstruct: street + homesite + city
                address_str = f"{parts_before_parenthesis}{parts_after_parenthesis} {homesite_part}{other_part}"

                # Convert HS to HomeSite in the moved part
                address_str = address_str.replace("HS", "APT", 1)
                address_str = address_str.replace("#", "", 1)

            # Update start index to continue searching
            # start_idx = len(street_part) + len(homesite_part) + 1

    # Remove remaining parentheses (for other cases)
    address_str = address_str.replace("(", "")
    address_str = address_str.replace(")", "")

    return address_str

def get_address_components(address: str, logger: logging.Logger | None = None) -> Dict[str, str]:
    try:
        # Preprocess address string
        address = preprocess_address_str(address)

        components: Dict[str, str] = {}
        # Parse address string
        parsed_address = usaddress.tag(address)
        address_property_bag = parsed_address[0]

        address_type: str = parsed_address[1]
        if (address_type != AddressType.StreetAddress.value and address_type != AddressType.Intersection.value):
            raise ValueError(f"Invalid address type: {address_type} for address: {address}")

        # Extract components
        # Street address
        # Build street part with abbreviation normalization
        street_components = [
            address_property_bag.get("AddressNumber", ""),
            address_property_bag.get("AddressNumberPrefix", ""),
            _abbreviate_word(address_property_bag.get("StreetNamePreDirectional", ""), directional_abbr),
            address_property_bag.get("StreetNamePreType", ""),
            address_property_bag.get("StreetName", ""),
            _abbreviate_word(address_property_bag.get("StreetNamePostType", ""), suffix_abbr),
            _abbreviate_word(address_property_bag.get("StreetNamePostDirectional", ""), directional_abbr)
        ]

        street = " ".join(filter(None, street_components))
        components["street"] = street

        # Unit information
        # Build unit part
        unit_components = [
            _abbreviate_word(address_property_bag.get("OccupancyType", ""), unit_abbr),
            address_property_bag.get("OccupancyIdentifier", "")
        ]

        # Handle # and Apt/Unit variations, # 116 -> APT 116
        if unit_components[0] == "" and unit_components[1].find("#") != -1:
            unit_components[0] = "APT"
            unit_components[1] = unit_components[1].replace("#", "").strip()
        unit = " ".join(filter(None, unit_components))
        if unit:
            components["unit"] = unit

        # City, state, zip
        city = address_property_bag.get("PlaceName", "")
        components["city"] = city

        state = address_property_bag.get("StateName", "")
        components["state"] = state

        zipcode = address_property_bag.get("ZipCode", "")
        components["zipcode"] = zipcode
        return components
    except Exception as e:
        # Fallback: normalize the original string if parsing fails
        error_msg = f"Error parsing address: {address}, error: {e}"
        if logger != None:
            logger.error(error_msg)
        else:
            print(error_msg)
        raise InvalidAddressError(error_msg, address)

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
        self._street_name: str = components["street"]
        self._unit: str = components.get("unit", "")

        if components.get("city") is None or components.get("city") == "":
            raise ValueError(f"Invalid address: {address}. City is required.")
        self._city: str = components["city"]

        if components.get("state") is None or components.get("state") == "":
            raise ValueError(f"Invalid address: {address}. State is required.")
        self._state: str = components["state"]

        if components.get("zipcode") is None or components.get("zipcode") == "":
            raise ValueError(f"Invalid address: {address}. Zip code is required.")
        self._zip_code: str = components["zipcode"]

        self._address_hash: str = get_address_hash(address, logger)


    @property
    def street_name(self) -> str:
        return self._street_name

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
    def zip_code(self) -> str:
        return self._zip_code

    @property
    def address_hash(self) -> str:
        return self._address_hash

    # This is index related
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPropertyAddress):
            return NotImplemented
        return self._address_hash == other._address_hash

    def __str__(self) -> str:
        return f"AddressHash: {self._address_hash}, Street: {self.street_name}, UnitNumber(if any): {self._unit}, State: {self._state}, ZipCode: {self._zip_code}"

if __name__ == "__main__":
    address_str = "6910 Old Redmond Road unit 116, Redmond, WA 98052"
    print(f"Input: {address_str}, Hashed: {get_address_hash(address_str)}")