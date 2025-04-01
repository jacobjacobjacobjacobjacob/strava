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


def process_activity_dataframe(df: pd.DataFrame):
    # Convert 'month' column (e.g., '01' → 'Jan')
    df["month"] = df["month"].apply(lambda x: calendar.month_abbr[int(x)])

    # Convert "duration" to hours.
    df["duration"] = round((df["duration"] / 60), 0)

    # Add year column
    # df["date"] = pd.to_datetime(df["date"])
    # df["year"] = pd.to_datetime(df["date"]).dt.year.astype('int32')
    df["year"] = pd.to_datetime(df["date"]).dt.year.astype("int32")
    return df


def load_dataframes():
    db_manager = DatabaseManager()

    activities_df = db_manager.get_table_as_dataframe("activities")
    activities_df = process_activity_dataframe(activities_df)

    return activities_df
