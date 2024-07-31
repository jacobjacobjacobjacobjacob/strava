# strava/streams.py.py
import os
import requests
import logging
from dotenv import load_dotenv

from assets.config import setup_logging
from assets.utils import available_streams

# Load logging configuration
setup_logging()
logger = logging.getLogger(__name__)


def get_activity_stream(activity_id: str, stream_type: str) -> dict:
    """
    Retrieve stream data for a given activity from Strava's API.

    :param activity_id: The ID of the activity to retrieve streams for.
    :param stream_type: The type of stream to retrieve (e.g., "time", "distance").
    :return: Stream data for the specified stream type, or None if the stream is not found or an error occurs.
    """

    load_dotenv()
    access_token = os.getenv("STRAVA_ACCESS_TOKEN")

    streams_url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "keys": {stream_type},
        "key_by_type": True,
    }

    try:
        response = requests.get(streams_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        stream = data.get(stream_type)

        if stream:
            logger.info(f"Successfully retrieved {stream_type} stream.")
            logger.debug(f"Temperature stream data: {stream_type}")  # Debug output
        else:
            logger.warning(f"{stream_type} not found..")
            logger.warning(f"Available streams: {available_streams}")
        return stream
    except requests.exceptions.RequestException as e:
        logger.error(f"Error occurred while retrieving {stream_type} stream: %s", e)
        return None
