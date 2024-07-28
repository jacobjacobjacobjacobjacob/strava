# main.py
import pandas as pd
import os

from api.update_data import fetch_strava_data
from assets.utils import create_dataframe, save_to_csv
from processing import clean_data
from validation import validate_data


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

    csv_file = os.path.join(os.path.dirname(__file__), "..", "data", "raw_data.csv")
    raw_data = create_dataframe(csv_file)
    clean_df = clean_data(raw_data)

    # Save clean data to CSV
    save_to_csv(clean_df, "clean_data")
    errors = validate_data(clean_df)

    if errors:
        print("Validation errors found:")
        for error in errors:
            print(error)
        return None

    return clean_df


if __name__ == "__main__":
    fetch_strava_data()

    df = main()
    # pd.options.display.max_columns = 999
    # print(df.tail())
