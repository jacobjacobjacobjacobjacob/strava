# src/analysis/loading.py
import pandas as pd
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
from loguru import logger
import calendar

from src.db.db_manager import DatabaseManager

# Load DataFrames
db_manager = DatabaseManager()
activities_df = db_manager.get_table_as_dataframe("activities")


def process_activity_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the activities DataFrame by:
    - Converting month numbers to abbreviations (e.g., '01' → 'Jan')
    - Converting duration from minutes to hours
    - Extracting year from date
    - Doubling running cadence (since Strava reports per-leg cadence)
    """
    logger.debug("Processing activities DataFrame...")
    try:
        # Convert 'month' column (e.g., '01' → 'Jan')
        if "month" in df.columns:
            df["month"] = df["month"].apply(
                lambda x: calendar.month_abbr[int(x)] if str(x).isdigit() else x
            )

        # Convert "duration" to hours
        if "duration" in df.columns:
            df["duration"] = round((df["duration"] / 60), 0)

        # Add year column if 'date' exists
        if "date" in df.columns:
            df["year"] = pd.to_datetime(df["date"]).dt.year.astype("int32")

        # Running cadence is for one leg, so multiply by 2
        if "average_cadence" in df.columns:
            df.loc[df["sport_type"] == "Run", "average_cadence"] *= 2

    except Exception as e:
        logger.error(f"Error processing activity DataFrame: {e}")
        raise

    return df


def process_zones_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the zones DataFrame by:"""
    logger.debug("Processing zones DataFrame...")
    try:

        # Convert "time_in_zone" to minutes
        if "time_in_zone" in df.columns:
            df["time_in_zone"] = round((df["time_in_zone"] / 60), 0)

    except Exception as e:
        logger.error(f"Error processing activity DataFrame: {e}")
        raise

    return df
def load_dataframes() -> dict[str, pd.DataFrame]:
    """Loads and processes multiple DataFrames from the database."""
    logger.debug("Loading DataFrames from DB...")
    tables = {
        "activities": process_activity_dataframe,
        "zones": process_zones_dataframe,
    }

    dataframes = {}
    try:
        for table, process_func in tables.items():
            df = db_manager.get_table_as_dataframe(table)
            dataframes[table] = process_func(df)
        
        return dataframes
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise

