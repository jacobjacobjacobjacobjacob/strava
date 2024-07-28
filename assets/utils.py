# assets/utils.py
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


def save_to_csv(df: pd.DataFrame, file_name: str) -> None:
    """
    Save a DataFrame to a CSV file.
    """
    try:
        base_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "strava", "data")
        )

        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
            logger.info(f"Created directory: {base_dir}")

        file_path = os.path.join(base_dir, f"{file_name}.csv")

        # Save DataFrame to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"DataFrame successfully saved to {file_path}")

    except Exception as e:
        logger.error(f"Failed to save DataFrame to CSV: {e}")
        raise


def create_dataframe(csv_file: str) -> pd.DataFrame:
    """
    Reads data from a CSV file and returns a pandas DataFrame.

    :param
        csv_file (str): Path to the CSV file.

    :return
        pd.DataFrame: DataFrame with the loaded data, or None if file not found.
    """
    try:
        df = pd.read_csv(csv_file)
        return df
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
        return None


def m_to_km(m: float) -> float:
    """
    Convert meters to kilometers.

    :param
        m (float): Distance in meters.

    :return
        float: Distance in kilometers.
    """
    return m / 1000


def ms_to_kph(ms: float) -> float:
    """
    Convert meters per second to kilometers per hour.

    :param
        ms (float): Speed in meters per second.

    :return
        float: Speed in kilometers per hour.
    """
    return ms * 3.6


def sec_to_h(sec: float) -> float:
    """
    Convert seconds to hours.

    :param
        sec (float): Duration in seconds.

    :return
        float: Duration in hours.
    """
    return sec / 3600
