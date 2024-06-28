import time
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from io import BytesIO
from common.s3_utils import S3Utility
from common.emailer import Emailer
from common.hashing import has_data_changed
from common.config_utils import load_config

def generate_line_plot(
    df: pd.DataFrame,
    x_axis_col: str,
    y_axis_col: str,
    x_label: str,
    y_label: str,
    x_label_interval: int,
    title: str,
    marker='o',
    file_format='png'
):
    """
    :params:
    df: Pandas DataFrame
    x_axis_col: x-axis column name
    y_axis_col: y-axis column name
    x_label: x-axis label
    y_label: y-axis label
    title: title
    marker: line plot marker
    file_format: file format such as 'png', 'jpg', etc.

    :returns:
    A BytesIO-wrapped plt plot in the specified format.
    """
    # Generate plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df[x_axis_col], df[y_axis_col], marker=marker)

    # Format axes
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=x_label_interval))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.gcf().autofmt_xdate() # auto format date labels to prevent overlap
    plt.xticks(rotation=0, ha='center')  # rotate to horizontal, set alignment to center
    ax.grid(True)

    # Labels
    plt.title(title)
    if x_label:
        plt.xlabel(x_label)
    if y_label:
        plt.ylabel(y_label)

    # Save plot to a BytesIO object in memory
    plot_buffer = BytesIO()
    plt.savefig(plot_buffer, format=file_format, bbox_inches='tight')
    plot_buffer.seek(0)  # Move the cursor to the beginning of the BytesIO object

    # Close the plot figure to free up memory
    plt.close(fig)

    return plot_buffer

def main():
    # Config
    config = load_config("primary-dealers/config.yml") # Run from base directory
    ## S3
    s3_bucket = config['aws']['s3_bucket']
    s3_key = config['aws']['s3_key'] # tlg/scrapers/primary-dealers
    ## API
    api_version = config['api']['version'] # e.g. 1.0
    print(f"API version: {api_version}")
    api_base_url = config['api']['base_url']
    api_file_format = config['api']['format'] # e.g. xlsx
    sleep_time = config['api']['sleep_time']
    api_key_ids = config['api']['key_ids']
    ## Email
    sender = config['email']['sender']
    gmail_app_password = config['email']['gmail_app_password']
    recipients = config['email']['recipients']

    # Loop through endpoints
    # Job will use start_year = 2022; get_historical.py uses 5 versions of the API
    start_year = 2022
    end_year = 9999
    s3 = S3Utility()
    for key_id in api_key_ids.values():
        # Issue GET request
        endpoint = api_base_url.format(start_year=start_year, key_id=key_id, format=api_file_format)
        print(f"Getting data from endpoint: {endpoint}")
        try:
            response = requests.get(endpoint)
            print("Response Code: ", response.status_code)
        except Exception as e:
            print("Did not get a response code from the endpoint.")
        if response.status_code == 200:
            data = response.content
        else:
            print("Failed to fetch data; continuing to next endpoint.")
            continue
        
        # Check if raw data has been updated by comparing GET content's MD5...
        # to existing S3 file's etag (which is also the MD5 hash)
        data_filename = f'{key_id}_{start_year}-{end_year}.{api_file_format}'
        s3_key_data = f'{s3_key}/data/{data_filename}'
        data_changed = has_data_changed(s3, s3_bucket, s3_key_data, data)
        
        if data_changed:
            print(f"{key_id} has changed.")
            # Upload to S3
            print(f"S3 destination: {s3_bucket}/{s3_key_data}")
            s3.upload_obj_s3(s3_bucket, s3_key_data, data)

            # Plot and email only for PDPOSGST-TOT
            if key_id == 'PDPOSGST-TOT':
                # Prepare data for plotting
                usecols = ['As Of Date', 'Value (millions)']
                df = pd.read_excel(BytesIO(data), usecols=usecols)
                df.columns = ['Date', '$ (millions)']
                df['$ (billions)'] = (df['$ (millions)']/1000).astype('int')
                df['Date'] = pd.to_datetime(df['Date'])
                df = df[::-1] # Invert rows

                # Make plot
                plot_file_format = 'png'
                plot = generate_line_plot( # Returns BytesIO object
                    df=df,
                    x_axis_col='Date',
                    y_axis_col='$ (billions)',
                    x_label=None,
                    y_label='$ (billions)',
                    x_label_interval=4,
                    title='Primary Dealer Net Outright Position: US Government Securities (excluding TIPS)',
                    marker=',',
                    file_format=plot_file_format
                )

                # Upload plot to S3
                plot_filename = f'{key_id}_{start_year}-{end_year}.{plot_file_format}'
                s3_key_plot = f'{s3_key}/plots/{plot_filename}'
                s3.upload_obj_s3(s3_bucket, s3_key_plot, plot)
                
                # Email
                ## Data attachment
                xlsx_buffer = BytesIO()
                df.to_excel(xlsx_buffer, index=False, engine='openpyxl')
                xlsx_buffer.seek(0)
                ## Data header
                df_header_string = df.head().to_string(index=False)
                ## Format contents
                emailer = Emailer(sender, gmail_app_password)
                title = "TLG - Primary Dealers Statistics"
                body = emailer.create_email_body_primary_dealers(df_header_string)
                ## Send email
                emailer.send_gmail(
                    recipients=recipients,
                    title=title,
                    body=body,
                    inline_buffer=plot,
                    #attachments={xlsx_buffer: '.xlsx'}
                )
        print(f"Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()