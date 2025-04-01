import unittest
import usaddress # type: ignore
from housetracker.iproperty import concatenateStreetAddress

class Test_concatenateStreetAddress(unittest.TestCase):
    def testStreetAddress(self):
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            addressObj = usaddress.tag(fullAddress)[0]
            streetAddress = concatenateStreetAddress(addressObj)
            self.assertEqual(expectedStreetAddress, streetAddress)

if __name__ == '__main__':
    unittest.main()