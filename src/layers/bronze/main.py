import re
import time
import boto3
import hashlib
from bs4 import BeautifulSoup
from layers.bronze.webscraper import Webscraper
from common.s3_utils import S3Utility
from common.emailer import Emailer
from common.config_utils import load_config

################################################################################
# Hashing
################################################################################
def compute_md5(obj: str) -> str:
    """Calculate the MD5 hash of a string object."""
    hash_md5 = hashlib.md5()
    hash_md5.update(obj)
    return hash_md5.hexdigest()

################################################################################
# PBOC scraping
################################################################################
def extract_download_paths(html: str, file_extension: str) -> list:
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
                    if link['href'].endswith(f'.{file_extension}'):
                        name_link_dict[name] = link['href']
    return name_link_dict

def construct_pboc_url(base_url: str, insertion: str) -> str:
    # Find the index of the last occurrence of "/"
    last_slash_index = base_url.rfind('/')
    
    # Split the string into before and after "/"
    before_last_slash = base_url[:last_slash_index + 1]  # Include the "/"
    after_last_slash = base_url[last_slash_index + 1:]
        
    return before_last_slash + insertion + "/" + after_last_slash


if __name__ == "__main__":
    # Config
    config = load_config("common/config.yml") # Run from base directory
    ## S3
    s3_bucket = config['aws']['s3_bucket']
    s3_key = config['aws']['pboc']['s3_bronze_key']
    ## Webscraping
    pboc_base_url = config['webscraping']['urls']['pboc']['base']
    base_url_2024 = config['webscraping']['urls']['pboc']['2024']['base']
    extensions_2024 = list(config['webscraping']['urls']['pboc']['2024']['extensions'].values())
    ## Email
    sender = config['email']['sender']
    gmail_app_password = config['email']['gmail_app_password']
    recipients = config['email']['recipients']

    # Construct URLs
    urls = []
    for extension in extensions_2024:
        url = construct_pboc_url(base_url_2024, extension)
        urls.append(url)

    # Get .xlsx download paths for each page
    webscraper = Webscraper()
    for url in urls:
        response = webscraper.get_request(url)
        html = response.text
        print(f"Downloaded html for {url}")
        if 'xlsx_paths' in locals():
            xlsx_paths.update(extract_download_paths(html, 'xlsx'))
        else:
            xlsx_paths = extract_download_paths(html, 'xlsx')
        print("xlsx paths extracted. Sleeping for 3 seconds...")
        time.sleep(3)
    
    # Download .xlsx files
    spreadsheets = dict()
    for table_name, path in xlsx_paths.items():
        url = pboc_base_url + path
        cleaned_table_name = re.sub(r'[^a-zA-Z\s]', '', table_name).lower().replace(' ', '_')
        print(f"Downloading {cleaned_table_name} from {url}...")
        response = webscraper.get_request(url)
        spreadsheet = response.content
        spreadsheets[cleaned_table_name] = spreadsheet
        print("Sleeping for 3 seconds...")
        time.sleep(3)

    # Compare md5 hashes and upload to S3 if spreadsheet has changed
    s3 = S3Utility()
    updated_spreadsheets = dict()
    for cleaned_table_name, spreadsheet in spreadsheets.items():
        # Get ETag of most recent upload of the spreadsheet
        s3_prefix = f'{s3_key}{cleaned_table_name}'
        latest_file_key = s3.get_latest_file_key(s3_bucket, s3_prefix)
        if latest_file_key:
            # Compute hashes for new and existing spreadsheets
            existing_spreadsheet_md5 = s3.download_etag(s3_bucket, latest_file_key)
            new_spreadsheet_md5 = compute_md5(spreadsheet)
            # Upload to S3 if spreadsheet has changed
            if existing_spreadsheet_md5 != new_spreadsheet_md5:
                print(f"{cleaned_table_name} has changed!")
                updated_spreadsheets[cleaned_table_name] = spreadsheet
                new_file_key = s3.replace_timestamp_in_filename(latest_file_key)
                s3.upload_obj_s3(s3_bucket, new_file_key, spreadsheet)
            else:
                print(f"No changes for {s3_prefix}")
        else:
            print(f"File does not exist: {latest_file_key}")
    
    # NOTE: do not email at bronze layer in handler; 
    # should ultimately be done at gold after aggregations
    if updated_spreadsheets:
        # Email any updated spreadsheets
        emailer = Emailer(sender, gmail_app_password)
        # Write changed spreadsheets to /tmp
        file_paths = []
        for cleaned_table_name, spreadsheet in updated_spreadsheets.items():
            file_path = f'tmp/{cleaned_table_name}.xlsx'
            file_paths.append(file_path)
            with open(file_path, 'wb') as file:
                file.write(spreadsheet)
        # Email contents
        title = "PBOC Data Update"
        body = "Updated PBOC tables are attached."
        # Send email
        emailer.send_gmail(
            recipients=recipients,
            title=title,
            body=body,
            attachments=file_paths
        )
        print("Successfully sent email!")
    else:
        print("No updates; did not send email.")


