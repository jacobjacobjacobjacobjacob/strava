# api/update_data.py
import os
import pandas as pd
import logging
from assets.config import setup_logging
from api.api import get_strava_activities

"""
Fetches new data from the API and writes it to a CSV-file. 
"""

setup_logging()
logger = logging.getLogger(__name__)


def fetch_strava_data() -> None:
    """
    Fetches Strava data from the API and saves it to a CSV file.
    """
    logger.info("Fetching Strava data...")

    strava_data = get_strava_activities()

    if strava_data is None:
        raise Exception("Failed to fetch activities.")

    df = pd.DataFrame(strava_data)

    if not df.empty:
        df.to_csv("data/raw_data.csv")
    else:
        logger.warning("No data retrieved. DataFrame is empty.")
