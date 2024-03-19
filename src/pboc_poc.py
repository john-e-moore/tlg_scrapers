import time
from src.utils.webscraping import download_html, download_xlsx, extract_links

if __name__ == "__main__":
    # In the 2024 Money and Banking Statistics page, find the href for .xlsx download
    # and download file
    # Stage raw .xlsx in S3
    # Store hash of raw table next to it
    # Clean table
    # Write cleaned table to S3
    # Check hash of downloaded table to existing latest hash
    # If changed, send email containing date
    # Deploy to Lambda on 1-day schedule

    # Later feature: look for year link from Statistics -> Data page
    # For now, just use 2024

    # Fetch html for 2024 Money and Banking Statistics landing page.
    mbs_url = "http://www.pbc.gov.cn/en/3688247/3688975/5242368/5242424/index.html"
    html = download_html(mbs_url)
    # Extract {table name: url path}
    xlsx_links = extract_links(html)
    base_url = "http://www.pbc.gov.cn"
    for table_name,url_path in xlsx_links.items():
        print(f"{table_name}: {url_path}")
        url = base_url + url_path
        print(f"URL: {url}")
        output_path = f"{table_name.lower().replace(' ', '_')}.xlsx"
        download_xlsx(url, output_path)
        print(f"Saved {table_name} to {output_path}")
        time.sleep(3)


    """
    #df = fetch_table(url, 5)
    # Clean spacing and non-ASCII characters
    df = df.applymap(clean_text)
    df.columns = [clean_text(x) for x in df.columns]
    # Trim unnecessary rows
    idx = df[df['Item'] == 'Total Liabilities'].index[0]
    df = df.iloc[1:idx+1,:]
    df.to_csv('balance_sheet.csv')
    print(df.head())
    print(df.shape)
    print(df.dtypes)
    """