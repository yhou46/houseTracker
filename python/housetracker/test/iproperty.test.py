import unittest
import usaddress # type: ignore

from housetracker.iproperty import extractStreetAddress, extractUnitInformation, IPropertyAddress

class Test_extractStreetAddress(unittest.TestCase):
    def testStreetAddress(self):
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd",
            "655 Crockett St Unit A101,Seattle, WA 98109": "655 Crockett St",
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

if __name__ == '__main__':
    unittest.main()