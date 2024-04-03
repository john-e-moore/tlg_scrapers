import json
from .main import main

def lambda_handler(event, context):
    # Assuming your scraping and processing logic is encapsulated in functions
    try:
        # Call the main scraping and processing functions here
        # For example, this could initiate a scrape and process the results
        response = main()

        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }
    except Exception as e:
        print(f"Error during scrape and process: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }