import unittest
import usaddress
from datetime import datetime

from shared.iproperty import IPropertyHistory, IPropertyHistoryEvent, PropertyHistoryEventType
from shared.iproperty_address import IPropertyAddress, get_address_components

class Test_get_address(unittest.TestCase):
    def test_street_address(self) -> None:
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd",
            "655 Crockett St Unit A101,Seattle, WA 98109": "655 Crockett St",
            "6928 155th Pl SE,Snohomish, WA 98296": "6928 155th Pl SE", # These 2 should be the same but test fails
            "6928 155th Place SE, Snohomish, WA 98296": "6928 155th Pl SE",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            components = get_address_components(fullAddress)
            streetAddress = components["street"]
            self.assertEqual(expectedStreetAddress, streetAddress)
    
    def test_unit(self) -> None:
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "APT 116",
            "655 Crockett St Unit A101,Seattle, WA 98109": "APT A101",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            components = get_address_components(fullAddress)
            unit  = components.get("unit", "")
            self.assertEqual(expectedStreetAddress, unit)

class Test_IPropertyHistory(unittest.TestCase):
    def test_history_events(self) -> None:
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")
        events = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=1000000),
            IPropertyHistoryEvent("event2", datetime(2022, 3, 1), PropertyHistoryEventType.Sold, "Sold", price=940000),
        ]
        history = IPropertyHistory("test-id", address, events)
        self.assertEqual(len(history._history), 2)
        history.addEvent(IPropertyHistoryEvent("event3", datetime(2022, 2, 1), PropertyHistoryEventType.PriceChange, "Price dropped", price=950000))
        self.assertEqual(len(history._history), 3)

        self.assertEqual(history._history[2]._event_type, PropertyHistoryEventType.Sold)

if __name__ == '__main__':
    unittest.main()