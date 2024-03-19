import time
import sys
from src.utils.webscraping import download_html, download_xlsx, extract_links, construct_pboc_url
from src.utils.storage import load_config

if __name__ == "__main__":
    # TODO: allow user to choose years

    # Config
    config = load_config("../config.yml")
    storage_config = config['storage']['pboc']
    urls = config['webscraping']['pboc']['2024']
