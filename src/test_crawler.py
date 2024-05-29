import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
url = 'https://www.redfin.com/zipcode/98052'
response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Page fetched successfully!")
else:
    print(f"Failed to fetch page: {response.status_code}")

soup = BeautifulSoup(response.text, 'html.parser')

properties = []
listings = soup.find_all('div', class_='HomeCardContainer')  # Update class as needed

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
