# api/api.py
import requests
from assets.config import setup_logging
from api.auth import get_strava_tokens
from assets.utils import save_to_csv


setup_logging()


def get_strava_activities():
    """Fetches all Strava activities"""
    access_token = get_strava_tokens()
    if access_token is None:
        raise Exception("Failed to retrieve access token.")
    activities_url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": "Bearer " + access_token}
    params = {"per_page": 200, "page": 1}

    try:
        response = requests.get(activities_url, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for non-200 status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Error occurred while fetching activities: %s", e)
        return None
