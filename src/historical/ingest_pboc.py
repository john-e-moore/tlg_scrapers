import re
import time
from utils.webscraping import download_html, download_xlsx, extract_paths, construct_pboc_url
from utils.config import load_config

if __name__ == "__main__":
    # TODO: allow user to choose years; only doing 2024 for now

    # Config
    config = load_config("config.yml") # Run from base directory
    s3_bucket = config['storage']['pboc']['s3_bucket']
    s3_key = config['storage']['pboc']['s3_bronze_key']
    pboc_base_url = config['webscraping']['urls']['pboc']['base']
    base_url_2024 = config['webscraping']['urls']['pboc']['2024']['base']
    extensions_2024 = list(config['webscraping']['urls']['pboc']['2024']['extensions'].values())

    # Construct URLs
    urls = []
    for extension in extensions_2024:
        url = construct_pboc_url(base_url_2024, extension)
        urls.append(url)

    # Get .xlsx download paths for each page
    for url in urls:
        html = download_html(url)
        print(f"Downloaded html for {url}")
        if 'xlsx_paths' in locals():
            xlsx_paths.update(extract_paths(html))
        else:
            xlsx_paths = extract_paths(html)
        print("xlsx paths extracted. Sleeping for 3 seconds...")
        time.sleep(3)

    # Download .xlsx files and write to S3
    for table_name, path in xlsx_paths.items():
        url = pboc_base_url + path
        cleaned_table_name = re.sub(r'[^a-zA-Z\s]', '', table_name).lower().replace(' ', '_')
        print(f"Downloading {cleaned_table_name} from {url}...")
        success = download_xlsx(url, cleaned_table_name, s3_bucket, s3_key)
        print("Sleeping for 3 seconds...")
        time.sleep(3)

    print("Finished.")
    
