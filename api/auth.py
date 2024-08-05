# api/auth.py
import os
import requests
import logging
from dotenv import load_dotenv, set_key
from assets.config import setup_logging

# Load logging configuration
setup_logging()
logger = logging.getLogger(__name__)

# Load credentials from .env file
load_dotenv()
strava_client_id = os.getenv("STRAVA_CLIENT_ID")
strava_client_secret = os.getenv("STRAVA_CLIENT_SECRET")
strava_refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")
strava_access_token = os.getenv("STRAVA_ACCESS_TOKEN")


def get_strava_tokens() -> str:
    """
    Fetches a new Strava access token using the refresh token and updates the .env file.

    :return: Access token as a string if successful, None otherwise.
    """
    token_url = "https://www.strava.com/oauth/token"
    refresh_payload = {
        "client_id": strava_client_id,
        "client_secret": strava_client_secret,
        "refresh_token": strava_refresh_token,
        "grant_type": "refresh_token",
    }

    try:
        response = requests.post(token_url, data=refresh_payload)
        response.raise_for_status()  # Raise exception for non-200 status codes
        data = response.json()
        access_token = data.get("access_token")

        if access_token:
            logger.info("Successfully retrieved new access token.")

            # Load existing .env file and update the access token
            env_file = "api/.env"
            load_dotenv(env_file)
            set_key(env_file, "token", access_token)
            logger.info("Access token written to .env file.")
        else:
            logger.warning("Access token not found in the response.")
        return access_token
    except requests.exceptions.RequestException as e:
        logger.error("Error occurred while refreshing token: %s", e)
        return None


if __name__ == "__main__":
    # Check for missing env varrs
    if not all([strava_client_id, strava_client_secret, strava_refresh_token]):
        logger.error("Strava credentials missing.")

    token = get_strava_tokens()

    if not token:
        logger.error("Failed to retrieve access token.")
