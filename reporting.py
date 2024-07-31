# reporting.py
import pandas as pd
import logging

from main import main
from assets.utils import all_months, month_mapping


# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def filter_sport_data(
    df: pd.DataFrame, sport_type: str, ride_type: str = None
) -> pd.DataFrame:
    """
    Filter the DataFrame based on sport type and ride type.

    :param
        df (pd.DataFrame): The DataFrame containing Strava data.
        sport (str): The sport type to filter by ("running" or "cycling").
        ride_type (str, optional): The ride type to filter by ("indoor" or "outdoor"). Only applicable for cycling.

    :returns
        pd.DataFrame: Filtered DataFrame.
    """
    if sport_type == "run":
        return df[df["sport_type"] == "run"]
    elif sport_type == "bike":
        if ride_type:
            return df[(df["sport_type"] == "bike") & (df["ride_type"] == ride_type)]
        else:
            return df[df["sport_type"] == "bike"]
    elif sport_type == "walk":
        return df[df["sport_type"] == "walk"]
    elif sport_type == "hike":
        return df[df["sport_type"] == "hike"]

    else:
        raise ValueError("Invalid sport type. Choose 'walk', 'hike', 'bike' or 'run'")


def monthly_summary(
    df: pd.DataFrame, month: str = None, sport_type: str = None, ride_type: str = None
) -> dict:
    """
    Generate a summary of activities for the given month.

    :param df: DataFrame containing Strava data.
    :param month: month in mmm-format, e.g. "jan" or "nov". If None, include all activities.
    :param sport_type: "cycling" or "running". If None, includes all activities.
    :param ride_type: "indoor" or "outdoor". Only applicable for cycling. If None, includes all ride types.
    :return: dict: summary of activities for the given month.
    """

    # Check if the month is valid and filter by month if provided
    if month:
        if month.lower() not in all_months:
            raise ValueError(
                "Invalid month. Please provide month in 'mmm' format, e.g., 'jan' or 'nov'."
            )
        else:
            df_month = df[df["month"] == month]
    else:
        df_month = df
        logger.info("'month' not provided, all data included.")

    # Apply additional filters for sport_type and ride_type
    if sport_type:
        df_month = df_month[df_month["sport_type"] == sport_type]

        if sport_type == "bike" and ride_type:
            df_month = df_month[df_month["ride_type"] == ride_type]

    # Ignore zero values for averages
    def mean_ignore_zeros(series: pd.Series) -> pd.Series:
        non_zero_values = series[series > 0]
        return (
            round(float(non_zero_values.mean()), 2)
            if not non_zero_values.empty
            else 0.0
        )

    # Generate summary statistics
    summary = {
        "total_activities": len(df_month),
        "total_distance_km": round(float(df_month["distance"].sum()), 2),
        "total_duration_h": round(float(df_month["duration"].sum()), 2),
        "total_elevation_gain_m": round(float(df_month["elevation_gain"].sum()), 2),
        "average_speed_kph": mean_ignore_zeros(df_month["average_speed"]),
        "max_speed_kph": round(float(df_month["max_speed"].max()), 2),
        "average_heartrate": mean_ignore_zeros(df_month["average_heartrate"]),
        "max_heartrate": round(float(df_month["max_heartrate"].max()), 2),
        "average_suffer_score": mean_ignore_zeros(df_month["suffer_score"]),
        "average_elevation_rate": mean_ignore_zeros(df_month["elevation_rate"]),
    }

    return summary


def cumsum_summary(
    df: pd.DataFrame, sport_type: str = None, ride_type: str = None
) -> pd.DataFrame:
    """
    Create a DataFrame with cumulative data by month.

    :param df: DataFrame containing Strava data.
    :param sport_type: "bike" or "run". If None, includes all activities.
    :param ride_type: "indoor" or "outdoor". Only applicable for cycling. If None, includes all ride types.
    :return: df: DataFrame with cumulative totals for each month.
    """

    # Filter by sport type
    if sport_type:
        df = df[df["sport_type"] == sport_type]

    # Further filter by ride type if provided
    if sport_type == "bike" and ride_type:
        df = df[df["ride_type"] == ride_type]

    columns_to_cumsum = [
        "distance",
        "duration",
        "elevation_gain",
        "suffer_score",
    ]

    cumsum_df = df[columns_to_cumsum].cumsum()

    if not all(col in df.columns for col in columns_to_cumsum):
        raise ValueError("One or more required columns are missing from the DataFrame.")

    # Convert month to datetime for proper sorting
    df.loc[:, "month"] = pd.to_datetime(df["month"], format="%b").dt.month

    # Group by month and sum
    monthly_sum_df = df.groupby("month")[columns_to_cumsum].sum().reset_index()
    monthly_sum_df = monthly_sum_df.sort_values("month")

    # Calculate cumsum
    cumsum_df = monthly_sum_df.copy()
    cumsum_df[columns_to_cumsum] = monthly_sum_df[columns_to_cumsum].cumsum()
    cumsum_df[columns_to_cumsum] = cumsum_df[columns_to_cumsum].round(1)

    # Add column for the number of activities
    activity_count = df.groupby("month")["id"].count().reset_index(name="activities")
    activity_count = activity_count.sort_values("month")

    cumsum_df["activities"] = activity_count["activities"].cumsum()

    # Map months to month names
    cumsum_df["month"] = cumsum_df["month"].map(month_mapping)

    return cumsum_df


def yearly_goal(df):
    pass


if __name__ == "__main__":
    df = main()

    pd.options.display.max_columns = None
    july = monthly_summary(df, month="jul", sport_type="bike", ride_type="outdoor")

    walking_df = filter_sport_data(df, "walk")
    hiking_df = filter_sport_data(df, "hike")
    running_df = filter_sport_data(df, "run")
    bike_df = filter_sport_data(df, "bike")
    indoor_bike_df = filter_sport_data(df, "bike", "indoor")
    outdoor_bike_df = filter_sport_data(df, "bike", "outdoor")

    cumsum_bike = cumsum_summary(df, sport_type="bike")
    cumsum_run = cumsum_summary(df, sport_type="run")
    cumsum_indoor = cumsum_summary(df, sport_type="bike", ride_type="indoor")

    print(cumsum_bike)
    # print(cumsum_run)

    # print("Walking Data:\n", walking_df)
    # print("Hiking Data:\n", hiking_df)
    # print("Running Data:\n", running_df)
    # print("Cycling Data:\n", cycling_df)
    # print("Indoor Cycling Data:\n", indoor_cycling_df)
    # print("Outdoor Cycling Data:\n", outdoor_cycling_df)
