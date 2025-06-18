import unittest
import usaddress # type: ignore
from datetime import datetime

from housetracker.iproperty import extractStreetAddress, extractUnitInformation, IPropertyAddress, IPropertyHistory, IPropertyHistoryEvent, IPropertyHistoryEventType

class Test_extractStreetAddress(unittest.TestCase):
    def testStreetAddress(self):
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd",
            "655 Crockett St Unit A101,Seattle, WA 98109": "655 Crockett St",
            "6928 155th Pl SE,Snohomish, WA 98296": "6928 155th Pl SE", # These 2 should be the same but test fails
            "6928 155th Place SE, Snohomish, WA 98296": "6928 155th Pl SE",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            addressObj = usaddress.tag(fullAddress)[0]
            streetAddress = extractStreetAddress(addressObj)
            self.assertEqual(expectedStreetAddress, streetAddress)
    
    def testUnit(self):
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "Apt 116",
            "655 Crockett St Unit A101,Seattle, WA 98109": "Unit A101",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            addressObj = usaddress.tag(fullAddress)[0]
            streetAddress = extractUnitInformation(addressObj)
            self.assertEqual(expectedStreetAddress, streetAddress)

class Test_IPropertyAddress(unittest.TestCase):
    def testFullAddressLine(self):
        # Input -> expected result
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St,Kirkland,WA,98033",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd,Apt 116,Redmond,WA,98052",
            "655 Crockett St Unit A101,Seattle, WA 98109": "655 Crockett St,Unit A101,Seattle,WA,98109",
        }

        for input, expected in testCases.items():
            addressObj = IPropertyAddress(input)
            self.assertEqual(expected, addressObj.getAddressLine())

class Test_IPropertyHistory(unittest.TestCase):
    def test_history_events(self):
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")
        events = [
            IPropertyHistoryEvent(datetime(2022, 1, 1), IPropertyHistoryEventType.Listed, "Listed", 1000000),
            IPropertyHistoryEvent(datetime(2022, 3, 1), IPropertyHistoryEventType.Sold, "Sold", 940000),
        ]
        history = IPropertyHistory("test-id", address, events)
        self.assertEqual(len(history._history), 2)
        history.addEvent(IPropertyHistoryEvent(datetime(2022, 2, 1), IPropertyHistoryEventType.PriceChange, "Price dropped", 950000))
        self.assertEqual(len(history._history), 3)

        self.assertEqual(history._history[2]._eventType, IPropertyHistoryEventType.Sold)

if __name__ == '__main__':
    unittest.main()