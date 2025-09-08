import requests

def get_html_content_from_url(url: str, save_to_filepath: str | None = None) -> str:
    """
    Fetch HTML content from a given URL.
    """
    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            html_content = response.text

            if save_to_filepath:
                # Save the page to specified file
                with open(save_to_filepath, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print(f"HTML content saved to: {save_to_filepath}")

            return html_content
        else:
            raise Exception(f"Failed to fetch page url: {url}, error code: {response.status_code}")
    except Exception as error:
        raise Exception(f"Error fetching URL {url}: {error}")
