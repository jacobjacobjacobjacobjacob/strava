# reporting.py
import pandas as pd

from main import main
from api.streams import get_activity_stream
from assets.utils import all_months, month_mapping
from assets.health_data import resting_hr, max_hr, weight_kg


def filter_sport_type(
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


def summary_totals(
    df: pd.DataFrame, month: str = None, year: int = None
) -> pd.DataFrame:
    """
    Generate a summary of activities for the given month.

    :param df: DataFrame containing Strava data.
    :param year: Optional year to filter the data.
    :param month: Optional month (in abbreviated form, f.ex "jan") to filter the data.
    :return: dict: summary of activities for the given month.
    """

    # Filtering
    if month:
        if month.lower() not in all_months:
            raise ValueError(
                "Invalid month. Please provide month in 'mmm' format, e.g., 'jan' or 'nov'."
            )

    df = df.copy()

    # Datetime
    df["date"] = pd.to_datetime(df["date"])
    df["year_month"] = df["date"].dt.to_period("M")

    # Group by year_month and aggregate
    summary_df = (
        df.groupby("year_month")
        .agg(
            total_activities=pd.NamedAgg(column="id", aggfunc="count"),
            total_distance_km=pd.NamedAgg(
                column="distance", aggfunc=lambda x: round(float(x.sum()), 1)
            ),
            total_duration_h=pd.NamedAgg(
                column="duration", aggfunc=lambda x: round(float(x.sum()), 1)
            ),
            total_elevation_gain_m=pd.NamedAgg(
                column="elevation_gain", aggfunc=lambda x: round(float(x.sum()), 0)
            ),
            vo2_max=pd.NamedAgg(
                column="vo2_max", aggfunc=lambda x: round(float(x.max()), 1)
            ),
        )
        .reset_index()
    )

    summary_df["vo2_max"] = summary_df["vo2_max"].round(1)

    # Split 'year_month' into 'year' and 'month'
    summary_df["year"] = summary_df["year_month"].dt.year
    summary_df["month"] = summary_df["year_month"].dt.month

    # Map months
    summary_df["month"] = summary_df["month"].map(month_mapping)

    summary_df = summary_df[
        [
            "year",
            "month",
            "total_activities",
            "total_distance_km",
            "total_duration_h",
            "total_elevation_gain_m",
            "vo2_max",
        ]
    ]

    return summary_df


def summary_averages(
    df: pd.DataFrame, month: str = None, year: int = None
) -> pd.DataFrame:
    """
    Generate a summary of averages for the given time period.

    :param df: DataFrame containing Strava data.
    :param year: Optional year to filter the data.
    :param month: Optional month (in abbreviated form, f.ex "jan") to filter the data.
    :return: dict: summary of averages for the given period.
    """

    # Filtering
    if month:
        if month.lower() not in all_months:
            raise ValueError(
                "Invalid month. Please provide month in 'mmm' format, e.g., 'jan' or 'nov'."
            )

    df = df.copy()

    # Datetime
    df["date"] = pd.to_datetime(df["date"])
    df["year_month"] = df["date"].dt.to_period("M")

    # Ignore zero values for calculating averages
    def mean_ignore_zeros(series: pd.Series) -> float:
        non_zero_values = series[series > 0]
        return (
            round(float(non_zero_values.mean()), 2)
            if not non_zero_values.empty
            else 0.0
        )

    average_summary_df = (
        df.groupby("year_month")
        .agg(
            average_speed_kph=pd.NamedAgg(
                column="average_speed", aggfunc=mean_ignore_zeros
            ),
            average_heartrate=pd.NamedAgg(
                column="average_heartrate", aggfunc=mean_ignore_zeros
            ),
            average_suffer_score=pd.NamedAgg(
                column="suffer_score", aggfunc=mean_ignore_zeros
            ),
            average_elevation_rate=pd.NamedAgg(
                column="elevation_rate", aggfunc=mean_ignore_zeros
            ),
            average_vo2_max=pd.NamedAgg(column="vo2_max", aggfunc=mean_ignore_zeros),
        )
        .reset_index()
    )

    # Split 'year_month' into 'year' and 'month'
    average_summary_df["year"] = average_summary_df["year_month"].dt.year
    average_summary_df["month"] = average_summary_df["year_month"].dt.month

    # Map months
    average_summary_df["month"] = average_summary_df["month"].map(month_mapping)

    average_summary_df = average_summary_df[
        [
            "year",
            "month",
            "average_speed_kph",
            "average_heartrate",
            "average_suffer_score",
            "average_elevation_rate",
            "average_vo2_max",
        ]
    ]

    return average_summary_df


def cumsum_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame with cumulative data by month.

    :param df: DataFrame containing Strava data.
    :param sport_type: "bike" or "run". If None, includes all activities.
    :param ride_type: "indoor" or "outdoor". Only applicable for cycling. If None, includes all ride types.
    :return: df: DataFrame with cumulative totals for each month.
    """

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


def create_yearly_goal_df(df: pd.DataFrame, goal_type: str, goal: int) -> pd.DataFrame:
    """
    Generate DataFrame for cumulative goal progress including all months.

    :param df: DataFrame containing cumulative data.
    :param goal_type: str, name of the column (e.g., "distance").
    :param goal: int, value of the yearly goal (e.g., 2000 km).
    :return: DataFrame with months, cumulative value for goal_type, and goal values.
    """

    # Calculate the monthly goal
    monthly_goal = goal / len(df)

    # Add a goal_distance column with cumulative values
    df[f"goal_{goal_type}"] = (df.index + 1) * monthly_goal

    # Ensure the DataFrame has all months listed
    all_months_df = pd.DataFrame({"month": all_months})

    # Merge data
    merged_df = all_months_df.merge(
        df[["month", goal_type, f"goal_{goal_type}"]], on="month", how="left"
    ).fillna(0)

    return merged_df


if __name__ == "__main__":
    df = main()

    walking_df = filter_sport_type(df, "walk")
    hiking_df = filter_sport_type(df, "hike")
    running_df = filter_sport_type(df, "run")
    bike_df = filter_sport_type(df, "bike")
    indoor_bike_df = filter_sport_type(df, "bike", "indoor")
    outdoor_bike_df = filter_sport_type(df, "bike", "outdoor")

    pd.options.display.max_columns = 100

    df_run = summary_totals(running_df, year=2024)
    # print(df_run)

    df_bike_2024 = summary_totals(outdoor_bike_df, year=2024, month="aug")
    print(df_bike_2024)

    average_df_bike_2024 = summary_averages(outdoor_bike_df, year=2024)
    # print(average_df_bike_2024)
    july_averages = summary_averages(outdoor_bike_df, year=2024, month="jul")

    cumsum_bike = cumsum_summary(bike_df)
    # print(cumsum_bike)

    distance_goal_df = create_yearly_goal_df(
        cumsum_bike, goal_type="distance", goal=2000
    )
