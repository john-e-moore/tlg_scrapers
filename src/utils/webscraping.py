import requests
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup

################################################################################
# 
################################################################################
def fetch_table(url: str, header: int) -> pd.DataFrame:
    """Fetch table from URL. Must be an html <table> for pd.read_html to recognize."""
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        html_io = StringIO(html)
        df_list = pd.read_html(io=html_io, header=header)
        return df_list[0]
    
    return None

################################################################################
# 
################################################################################
def download_html(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching html.\n{e}")
        return None

################################################################################
# 
################################################################################
def download_xlsx(url: str, output_filename: str, s3_bucket=None, s3_key=None) -> None:
    try:
        response = requests.get(url)
        response.raise_for_status()

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        if s3_bucket and s3_key:
            full_key = f'{s3_key}{output_filename}_{timestamp}.xlsx'
            try:
                s3 = boto3.client('s3')
                response = s3.put_object(Bucket=s3_bucket, Key=full_key, Body=response.content)
                print(f"Successfully uploaded: {response}")
                return True
            except ClientError as e:
                print(f"Client error occurred:\n{e}")
                return False
            except BotoCoreError as e:
                print(f"BotoCore error occurred:\n{e}")
                return False
            except Exception as e:
                print(f"Unexpected error occurred:\n{e}")
                return False
        else:
            full_output_filename = f'{output_filename}.xlsx'
            with open(full_output_filename, 'wb') as file:
                file.write(response.content)
                response = f"Successfully downloaded: {full_output_filename}"
            return True
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading xlsx.\n{e}")
        return False

################################################################################
# 
################################################################################
def extract_paths(html: str) -> list:
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

################################################################################
# 
################################################################################
def construct_pboc_url(base_url: str, insertion: str) -> str:
    # Find the index of the last occurrence of "/"
    last_slash_index = base_url.rfind('/')
    
    # Split the string into before and after "/"
    before_last_slash = base_url[:last_slash_index + 1]  # Include the "/"
    after_last_slash = base_url[last_slash_index + 1:]
        
    return before_last_slash + insertion + "/" + after_last_slash