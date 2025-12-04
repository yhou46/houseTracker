import unittest
from shared.iproperty_address import get_address_hash
from typing import List, Tuple

class TestGetAddressHash(unittest.TestCase):
    def test_normal_addresses(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            ("1838 Market St, Kirkland, WA 98033", "1838-market-st|kirkland|wa|98033"),
            ("7425 166th Ave NE c230, Redmond, WA 98052", "7425-166th-ave-ne|c230|redmond|wa|98052"),
            ("7301 NE 175th St,Kenmore, WA 98028", "7301-ne-175th-st|kenmore|wa|98028"),
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

    def test_addresses_with_unit_information(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            ("655 Crockett St Unit A107,Seattle, WA 98109", "655-crockett-st|apt-a107|seattle|wa|98109"),
            ("655 Crockett St APT A107, Seattle, WA 98109", "655-crockett-st|apt-a107|seattle|wa|98109"),
            ("6910 Old Redmond Rd #116,Redmond, WA 98052", "6910-old-redmond-rd|apt-116|redmond|wa|98052"),
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)

    def test_vacant_land_address(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            ("1203 X Dave Road, Redmond, WA 98052", "1203-x-dave-rd|redmond|wa|98052"),
            ("1203 X Dave Rd,Redmond, WA 98052", "1203-x-dave-rd|redmond|wa|98052"),
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)

    def test_addresses_with_special_marks(self) -> None:
        test_addresses: List[Tuple[str, str]] = [
            # HS or Homesite cases
            ("7988 170th Ave NE (Homesite #14),Redmond, WA 98052", "7988-170th-ave-ne|apt-14|redmond|wa|98052"),
            ("11170 (HS #24) NE 134th Ct NE, Redmond, WA 98052", "11170-ne-134th-ct-ne|apt-24|redmond|wa|98052"),
            ("13468 (HS #9) NE 112th Pl, Redmond, WA 98052", "13468-ne-112th-pl|apt-9|redmond|wa|98052"),
            ("11162 (HS 23) 134th Ct NE, Redmond, WA 98052", "11162-134th-ct-ne|apt-23|redmond|wa|98052"),
            ("13426 (HS 2) NE 112th Pl, Redmond, WA 98052", "13426-ne-112th-pl|apt-2|redmond|wa|98052"),
            ("13472 (HS 8) NE 112th Pl, Redmond, WA 98052", "13472-ne-112th-pl|apt-8|redmond|wa|98052"),
            ("13434 (HS 3) NE 112th Pl, Redmond, WA 98052", "13434-ne-112th-pl|apt-3|redmond|wa|98052"),

            # Duplicate unit cases, remove (HS xx) if unit info already present
            ("10814 (HS 65) 120TH Ln NE Unit E, Kirkland, WA 98033", "10814-120th-ln-ne|apt-e|kirkland|wa|98033"),

            # Private Lane case
            ("8533 NE Juanita Dr (Private Lane), Kirkland, WA 98034", "8533-ne-juanita-dr|kirkland|wa|98034"),
        ]
        for input_addr, expected_hash in test_addresses:
            print(f"Input: {input_addr}, Hashed: {get_address_hash(input_addr)}")
            self.assertEqual(get_address_hash(input_addr), expected_hash)


if __name__ == "__main__":
    unittest.main()