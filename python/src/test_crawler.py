import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
url = 'https://www.redfin.com/zipcode/98052'
propertyUrl = "https://www.redfin.com/WA/Kirkland/12419-106th-Pl-NE-98034/home/458245"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Page fetched successfully!")
else:
    print(f"Failed to fetch page: {response.status_code}")


soup = BeautifulSoup(response.text, 'html.parser')

properties = []
listings = soup.find_all('div', class_='HomeCardContainer')  # Update class as needed

for listing in listings:
    print(listing.prettify())  # Print the HTML of each listing
    try:
        price = listing.find('span', class_='homecardV2Price').text.strip()
        print(f"Price: {price}")
        address = listing.find('div', class_='address').text.strip()
        print(f"Address: {address}")
        beds_baths = listing.find('div', class_='stats').text.strip()
        print(f"Beds/Baths: {beds_baths}")
        properties.append({
            'price': price,
            'address': address,
            'beds_baths': beds_baths
        })
    except AttributeError as e:
        print(f"An error occurred: {e}")
        continue

for listing in listings:
    try:
        price = listing.find('span', class_='homecardV2Price').text.strip()  # Update class as needed
        address = listing.find('span', class_='homeAddressV2').text.strip()  # Update class as needed
        beds = listing.find('span', class_='homecardV2Stats').text.strip()  # Update class as needed
        properties.append({
            'price': price,
            'address': address,
            'beds': beds
        })
    except AttributeError:
        continue

df = pd.DataFrame(properties)
print(df)
