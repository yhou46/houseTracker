import requests
from bs4 import BeautifulSoup
import os

def getRedfinResponse(url: str):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Page fetched successfully!")

    else:
        print(f"Failed to fetch page: {response.status_code}")
        raise RuntimeError(f"Failed to get response from url: {url}")

    return response.text

def parseHtml (content: str):
    soup = BeautifulSoup(content)
    formatted = soup.prettify()

    with open("./formatted.txt", "w") as file:
        file.write(formatted)

    print(formatted)

if __name__ == "__main__":
    # Get page
    # url = 'https://www.redfin.com/zipcode/98052'
    # result = getRedfinResponse(url)

    # with open("./redfinResponse.txt", "w") as file:
    #     file.write(result.text)

    # parse page
    currentPath = os.path.dirname(__file__)
    print(currentPath)
    with open(f"{currentPath}/redfinResponse.txt", "r") as file:
        content = file.read()
    parseHtml(content)