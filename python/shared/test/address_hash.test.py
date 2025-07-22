import unittest
from shared.iproperty import get_address_hash
from typing import List, Tuple

class TestGetAddressHash(unittest.TestCase):
    def test_normal_addresses(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            ("1838 Market St, Kirkland, WA 98033", "1838-market-st|kirkland|wa|98033"),
            ("7425 166th Ave NE c230, Redmond, WA 98052", "7425-166th-ave-ne|c230|redmond|wa|98052"),
            ("6910 Old Redmond Rd Apt 116, Redmond, WA, 98052", "6910-old-redmond-rd|apt-116|redmond|wa|98052"),
            ("7301 NE 175th St,Kenmore, WA 98028", "7301-ne-175th-st|kenmore|wa|98028")
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)

    def test_addresses_with_different_order(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            #("c230, 7425 166th Ave NE, Redmond, WA 98052", "7425-166th-ave-ne|c230|redmond|wa|98052"),
            ("Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052", "6910-old-redmond-rd|apt-116|redmond|wa|98052")
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)

    def test_addresses_with_abbreviations(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            ("7301 NE 175th Street, Kenmore, WA 98028", "7301-ne-175th-st|kenmore|wa|98028")
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)

if __name__ == "__main__":
    unittest.main() 