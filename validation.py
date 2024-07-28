# validation.py
import pandas as pd
import logging

from assets.config import setup_logging

# Load logging configuration
setup_logging()
logger = logging.getLogger(__name__)


def check_ride_types(df: pd.DataFrame) -> list:
    """
    Checking that the ride types are correct (indoor/outdoor).

    :param
    df (pd.DataFrame): The DataFrame containing processed Strava data.

    :returns
    A list of errors found during validation.
    """

    errors = []
    for index, row in df.iterrows():
        if (row["type"] == "indoor") and (row["distance"] != 0):
            error_msg = f'Error: Indoor ride on {row["date"]} at index {index} has distance value {row["distance"]}.'
            errors.append(error_msg)
            logger.error(error_msg)
        elif (row["type"] == "outdoor") and (row["distance"] == 0):
            error_msg = f'Error: Outdoor ride on {row["date"]} at index {index} has no distance value.'
            errors.append(error_msg)
            logger.error(error_msg)

    return errors


def validate_data(df: pd.DataFrame) -> list:
    """
    Runs all validation functions on the given DataFrame.

    :param
    df (pd.DataFrame): The DataFrame containing processed Strava data.
    :returns
    A list of errors found during validation.
    """
    errors = []
    errors.extend(check_ride_types(df))

    return errors
