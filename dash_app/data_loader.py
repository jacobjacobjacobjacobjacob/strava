# dash_app/data_loader.py
import pandas as pd
from dash_app.components.schema import DataSchema


def load_strava_data(path: str) -> pd.DataFrame:
    data = pd.read_csv(
        path,
        dtype={
            DataSchema.ID: str,
            DataSchema.MONTH: str,
            DataSchema.YEAR: int,
            DataSchema.DAY_OF_WEEK: str,
            DataSchema.START_TIME: str,
            DataSchema.END_TIME: str,
            DataSchema.SUFFER_SCORE_BUCKET: str,
            DataSchema.SPORT_TYPE: str,
            DataSchema.RIDE_TYPE: str,
            DataSchema.DURATION: float,
            DataSchema.DISTANCE: float,
            DataSchema.ELEVATION_GAIN: float,
            DataSchema.AVERAGE_SPEED: float,
            DataSchema.MAX_SPEED: float,
            DataSchema.AVERAGE_HEARTRATE: float,
            DataSchema.MAX_HEARTRATE: float,
            DataSchema.SUFFER_SCORE: float,
            DataSchema.ELEVATION_RATE: float,
            DataSchema.AVERAGE_WATTS: float,
            DataSchema.VO2_MAX: float,
        },
        parse_dates=[DataSchema.DATE],
    )

    # Extract year from the date column
    data[DataSchema.YEAR] = data[DataSchema.DATE].dt.year

    def add_combined_column(data: pd.DataFrame) -> pd.DataFrame:
        # Ensure 'ride_type' is not NaN when 'sport_type' is 'bike'
        data["sport_type"] = data.apply(
            lambda row: (
                f"{row[DataSchema.SPORT_TYPE]} ({row[DataSchema.RIDE_TYPE]})"
                if row[DataSchema.SPORT_TYPE] == "bike"
                and pd.notna(row[DataSchema.RIDE_TYPE])
                else row[DataSchema.SPORT_TYPE]
            ),
            axis=1,
        )
        return data

    # Combine sport type and ride type for filtering bike rides
    data = add_combined_column(data)

    data.rename(columns={DataSchema.ELEVATION_GAIN: "elevation"}, inplace=True)

    return data
