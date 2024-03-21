import requests
from typing import Optional, Dict, Any

class Webscraper:

    def __init__(self, base_url: str, username: Optional[str] = None, password: Optional[str] = None, headers: Optional[Dict[str, str]] = None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.headers = headers

    def get_request(self, url: str, username: Optional[str] = None, password: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Issues a GET request to the specified URL.
        Allows optional passing of login credentials and request headers.
        """
        try:
            if username and password:
                response = requests.get(url, auth=(username, password), headers=headers)
            else:
                response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raises HTTPError for bad responses
            return response
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.ConnectionError as conn_err:
            print(f'Connection error occurred: {conn_err}')
        except requests.exceptions.Timeout as timeout_err:
            print(f'Timeout error occurred: {timeout_err}')
        except requests.exceptions.RequestException as req_err:
            print(f'Unexpected error occurred: {req_err}')

    def post_request(self, url: str, username: Optional[str] = None, password: Optional[str] = None, headers: Optional[Dict[str, str]] = None, data: Optional[Dict[str, Any]] = None) -> Any:
        """
        Issues a POST request to the specified URL.
        Allows optional passing of login credentials, request headers, and POST data.
        """
        try:
            if username and password:
                response = requests.post(url, auth=(username, password), headers=headers, data=data)
            else:
                response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()  # Raises HTTPError for bad responses
            return response
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.ConnectionError as conn_err:
            print(f'Connection error occurred: {conn_err}')
        except requests.exceptions.Timeout as timeout_err:
            print(f'Timeout error occurred: {timeout_err}')
        except requests.exceptions.RequestException as req_err:
            print(f'Unexpected error occurred: {req_err}')
