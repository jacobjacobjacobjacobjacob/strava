# reporting.py
import pandas as pd
from main import main

df = main()


def filter_sport_data(df: pd.DataFrame, sport_type: str = None, ride_type: str = None):
    """
    Filters sport activities from the Strava data.

    :param df: DataFrame containing Strava data.
    :param sport_type: "cycling" or "run". If None, includes all activities.
    :param ride_type: "indoor" or "outdoor". If None, includes all ride types.
    :return: df: DataFrame filtered by given sport and ride type.
    """

    if sport_type:
        if sport_type.lower() == "cycling":
            if ride_type:
                if ride_type.lower() == "indoor":
                    filtered_df = df[df["sport_type"] == "indoor"]
                elif ride_type.lower() == "outdoor":
                    filtered_df = df[df["sport_type"] == "outdoor"]
                else:
                    raise ValueError("ride_type must be 'indoor' or 'outdoor'")
            else:
                filtered_df = df[df["sport_type"].isin(["indoor", "outdoor"])]
        elif sport_type.lower() == "run":
            filtered_df = df[df["sport_type"] == "run"]
        else:
            raise ValueError("sport_type must be 'cycling' or 'run'")
    else:
        if ride_type:
            if ride_type.lower() == "indoor":
                filtered_df = df[df["sport_type"] == "indoor"]
            elif ride_type.lower() == "outdoor":
                filtered_df = df[df["sport_type"] == "outdoor"]
            else:
                raise ValueError("ride_type must be 'indoor' or 'outdoor'")
        else:
            filtered_df = df[df["sport_type"].isin(["indoor", "outdoor", "run"])]

    return filtered_df
