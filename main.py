# main.py
import pandas as pd
import os
import logging

from api.update_data import fetch_strava_data
from assets.utils import create_dataframe, save_to_csv
from processing import clean_data
from validation import validate_data
from database.main import engine, write_to_database

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(csv_file="data/raw_data.csv") -> pd.DataFrame:
    """
    Main function for processing Strava data.

    :param csv_file: str, optional
        Path to the CSV file containing raw data. Default is "data/raw_data.csv".

    Operations:
    - Processes, cleans and validates the data.
    - Writes the processed data to a CSV-file.

    :return:
    - full_df (DataFrame): Processed and validated dataset.
    """

    try:
        # Create DataFrame from CSV file
        raw_data = create_dataframe(csv_file)
        if raw_data.empty:
            logger.warning("The raw data is empty.")
            return None

        # Clean the raw data
        clean_df = clean_data(raw_data)

        # Save clean data to CSV
        save_to_csv(clean_df, "clean_data")

        # Validate the cleaned data
        errors = validate_data(clean_df)

        if errors:
            logger.error("Validation errors found:")
            for error in errors:
                logger.error(error)
            return None

        logger.info("Data processing completed successfully.")
        return clean_df

    except Exception as e:
        logger.error(f"An error occurred during processing: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    fetch_strava_data()

    df = main()

    if df is not None:
        write_to_database(df, "activities")
    else:
        logger.error("DataFrame processing failed.")
