# src/api/config.py
""" API configurations """
import os
from dotenv import load_dotenv

load_dotenv()
def get_strava_api_config():
    strava_config = {
        "client_id": os.getenv("STRAVA_CLIENT_ID"),
        "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
        "refresh_token": os.getenv("STRAVA_REFRESH_TOKEN"),
        "athlete_id": os.getenv("STRAVA_ATHLETE_ID"),
    }
    return strava_config
