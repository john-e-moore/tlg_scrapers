import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

def fetch_table(url: str, header: int) -> pd.DataFrame:
    """Fetch table from URL. Must be an html <table> for pd.read_html to recognize."""
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        html_io = StringIO(html)
        df_list = pd.read_html(io=html_io, header=header)
        return df_list[0]
    
    return None

def download_html(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching html.\n{e}")
        return None
    
def download_xlsx(url: str, output_path: str) -> None:
    try:
        response = requests.get(url)
        # Make sure response is an Excel file
        if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' not in response.headers.get('Content-Type', ''):
            print("The link did not direct to an Excel file.")
            return False
        response.raise_for_status()
        with open(output_path, 'wb') as file:
            file.write(response.content)
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading xlsx.\n{e}")
        return False
    
def extract_links(html: str) -> list:
    soup = BeautifulSoup(html, 'html.parser')
    name_link_dict = {}
    tables = soup.find_all('table', class_='data_table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if cells and len(cells) > 0:
                # Name is in first cell of row.
                name = cells[0].text.strip()
                links = row.find_all('a', href=True)
                for link in links:
                    if link['href'].endswith('.xlsx'):
                        name_link_dict[name] = link['href']
    return name_link_dict