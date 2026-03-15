''' Utility functions for API interactions. '''

import requests

from config.conf import FAOSTAT_BASE_URL, FAOSTAT_UNAME, FAOSTAT_PWD

#FAOSTAT API

def get_faostat_auth_token() -> str:
    ''' Fetches an authentication token from the FAOSTAT API. '''
    url = f"{FAOSTAT_BASE_URL}/auth/login"
    payload = {
        "username": FAOSTAT_UNAME,
        "password": FAOSTAT_PWD
    }
    headers = {
        "authorization": "Bearer ",
        "content-type": "application/x-www-form-urlencoded"
    }

    response = requests.post(url, data=payload, headers=headers, timeout=3)
    response.raise_for_status()  # Raise an exception for HTTP errors
    try:
        token = response.json()["AuthenticationResult"]["AccessToken"]
    except Exception as e:
        raise Exception(f"Failed to parse authentication token from response: {e}")
    return token