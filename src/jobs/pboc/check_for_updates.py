import re
import time
import boto3
from src.utils.webscraping import download_html, download_xlsx, extract_paths, construct_pboc_url
from src.utils.storage import load_config, get_most_recently_modified_file, upload_local_file_s3
from utils.processing import check_file_change

if __name__ == "__main__":
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

    # Download .xlsx files to /tmp
    local_files = []
    for table_name, path in xlsx_paths.items():
        url = pboc_base_url + path
        cleaned_table_name = re.sub(r'[^a-zA-Z\s]', '', table_name).lower().replace(' ', '_')
        print(f"Downloading {cleaned_table_name} from {url}...")
        output_filename = f'tmp/{cleaned_table_name}'
        success = download_xlsx(url, output_filename)
        local_files.append(f'{output_filename}.xlsx')
        print("Sleeping for 3 seconds...")
        time.sleep(3)

    # Check if files have changed
    changed_files = []
    for file in local_files:
        print(f"Checking {file} for updates...")
        cleaned_file_name = file.split('/')[1].split('.')[0]

        # Get most recent version of file in S3
        s3_prefix = f'{s3_key}{cleaned_file_name}'
        print(f"S3 prefix to check: {s3_prefix}")
        most_recent_file = get_most_recently_modified_file(s3_bucket, s3_prefix)
        print(f"Most recent file: {most_recent_file}")

        # Check if hashes match
        if most_recent_file:
            has_changed = check_file_change(file, s3_bucket, most_recent_file)
            if has_changed:
                print("File has changed; uploading to S3.")
                # Upload to S3
                upload_local_file_s3(file, s3_bucket, s3_key)
        else:
            print(f"No file found matching prefix {s3_prefix}")
        break # Testing



    


    # Download .xlsx to /tmp
    # Calculate MD5 hash
    # Compare hashes
