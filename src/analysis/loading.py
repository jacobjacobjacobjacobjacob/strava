# src/analysis/loading.py
import pandas as pd
from loguru import logger
import calendar

from src.db.db_manager import DatabaseManager

# Load DataFrames
db_manager = DatabaseManager()
activities_df = db_manager.get_table_as_dataframe("activities")

def process_activity_dataframe(df: pd.DataFrame):
    # Convert 'month' column (e.g., '01' → 'Jan')
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[int(x)])
    # Convert "duration" to hours.
    df["duration"] = round((df["duration"] / 60), 0)
    return df

def load_dataframes():
    db_manager = DatabaseManager()

    activities_df = db_manager.get_table_as_dataframe("activities")
    activities_df = process_activity_dataframe(activities_df)

    return activities_df
