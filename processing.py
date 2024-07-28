# processing.py
import pandas as pd

# import pytz
import os

from assets.utils import m_to_km, ms_to_kph, sec_to_h, create_dataframe
from assets.config import gear, local_tz
from validation import validate_data

"""
Processing Strava data to extract clean and relevant information:
1. Read the raw data from CSV.
2. Filter by activity type and date.
3. Convert various metrics into more readable formats.
4. Compile the cleaned data into DataFrames based on different filters.
"""


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns for readability and clarity.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with renamed columns.
    """

    if "gear_id" in df.columns:
        df.rename(columns={"gear_id": "ride_type"}, inplace=True)
    if "start_date" in df.columns:
        df.rename(columns={"start_date": "date"}, inplace=True)
    if "moving_time" in df.columns:
        df.rename(columns={"moving_time": "duration"}, inplace=True)
    if "total_elevation_gain" in df.columns:
        df.rename(columns={"total_elevation_gain": "elevation_gain"}, inplace=True)

    # Convert all strings in all rows to lowercase
    for column in df.select_dtypes(include=["object"]).columns:
        # Ensure the column contains string values
        if df[column].apply(lambda x: isinstance(x, str)).all():
            df[column] = df[column].str.lower()

    return df


def filter_by_period(
    df: pd.DataFrame,
    start_date: str = None,
    end_date: str = None,
    year: int = None,
    month: int = None,
) -> pd.DataFrame:
    """
    Filter DataFrame by time parameters.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.
        start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        end_date (str, optional): End date in 'YYYY-MM-DD' format.
        year (int, optional): Year to filter by.
        month (int, optional): Month to filter by.

    :returns
        pd.DataFrame: DataFrame filtered by given period.
    """

    # Sort by date
    df = df.sort_values(by="date", ascending=True)

    if start_date is not None:
        df = df[df["date"] >= start_date]
    if end_date is not None:
        df = df[df["date"] <= end_date]
    if year is not None:
        df = df[df["date"].dt.year == year]
    if month is not None:
        df = df[df["date"].dt.month == month]
    return df


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert units (distance, speed etc.) and formats date to datetime.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with converted units.
    """

    if "distance" in df.columns:
        df["distance"] = df["distance"].apply(m_to_km)
    if "duration" in df.columns:
        df["duration"] = df["duration"].apply(sec_to_h)
    if "average_speed" in df.columns:
        df["average_speed"] = df["average_speed"].apply(ms_to_kph)
    if "max_speed" in df.columns:
        df["max_speed"] = df["max_speed"].apply(ms_to_kph)

    # Convert to datetime-format.
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    return df


def filter_columns(cleaned_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the DataFrame to only keep the specified columns.

    :param
        cleaned_df (pd.DataFrame): The DataFrame containing cleaned Strava data.

    :returns
        pd.DataFrame: Filtered DataFrame with desired columns.
    """

    columns = {
        "all": [
            "id",
            "date",
            "month",
            "day_of_week",
            "start_time",
            "end_time",
            "duration",
            "distance",
            "elevation_gain",
            "average_speed",
            "max_speed",
            "average_heartrate",
            "max_heartrate",
            "suffer_score",
            "suffer_score_bucket",
            "elevation_ratio",
            "sport_type",
        ],
    }

    df = cleaned_df[columns["all"]].copy()

    return df


def add_month_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add "month"-column to the DataFrame.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with "month"-column added.
    """
    df["month"] = df["date"].dt.month_name().str.slice(stop=3).str.lower()
    return df


def add_day_of_week(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add "day_of_week"-column to the DataFrame.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with "day_of_week"-column added.
    """
    weekdays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    df["day_of_week"] = df["date"].dt.weekday.map(lambda x: weekdays[x])
    return df


def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Converts the "date"-column to the specified local timezone.
    - Add "start_time" and "end_time" columns based on duration.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with "start_time" and "end_time" columns added.
    """

    # Check if date column is timezone-aware
    if df["date"].dt.tz is None:
        # Localize to UTC
        df["date"] = df["date"].dt.tz_localize("UTC")

    # Convert to local timezone
    df["date"] = df["date"].dt.tz_convert(local_tz)

    # Reformat the date to YYYY-MM-DD
    df["date"] = df["date"].dt.date

    # Calculate end_time based on duration
    df["end_time"] = df["date"] + pd.to_timedelta(df["duration"], unit="h")

    df["start_time"] = df["date"].dt.strftime("%H:%M")
    df["end_time"] = df["end_time"].dt.strftime("%H:%M")

    return df


def add_elevation_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add "elevation_rate"-column (elevation gained per kilometer).

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with "elevation_rate"-column added.
    """
    df["elevation_rate"] = df.apply(
        lambda row: (
            (row["distance"] / row["elevation_gain"])
            if row["elevation_gain"] != 0
            else 0
        ),
        axis=1,
    )
    return df


def sort_and_reset_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort DataFrame by 'date' column and reset index.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: Sorted and reindexed DataFrame.
    """

    df = df.sort_values(by="date")

    df.reset_index(drop=True, inplace=True)
    df.index += 1  # Shift index to start from 1

    return df


def map_ride_type(df: pd.DataFrame, gear_dict) -> pd.DataFrame:
    """
    Map ride_type values based on gear dictionary and keep sport_type as "Run" unchanged.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.
        gear_dict (dict): Dictionary to map ride types.

    :returns
        pd.DataFrame: DataFrame with mapped ride types.
    """
    if "sport_type" in df.columns and "ride_type" in df.columns:
        df.loc[df["sport_type"] != "run", "sport_type"] = df["ride_type"].map(gear_dict)

    return df


def replace_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN values with the mean for columns with missing heart rate data.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.

    :returns
        pd.DataFrame: DataFrame with NaN values replaced.
    """
    columns_to_fill = ["max_heartrate", "average_heartrate", "suffer_score"]

    for column in columns_to_fill:
        if column in df.columns:
            mean_value = df[column].mean()
            df[column] = df[column].fillna(mean_value)

    return df


def add_suffer_score_buckets(df):
    labels = ["very low", "low", "medium", "high", "very high"]
    df["suffer_score_bucket"] = pd.qcut(df["suffer_score"], q=5, labels=labels)
    return df
