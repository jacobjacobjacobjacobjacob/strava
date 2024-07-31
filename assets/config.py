# assets/config.py
import pytz
import logging


# Strava gear_id's, representing different types of gear, in this case spinning bike/outdoor road bike.
gear = {
    "b14008209": "indoor",
    "b12658885": "outdoor",
}

local_tz = pytz.timezone("Europe/Oslo")  # Timezone


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
