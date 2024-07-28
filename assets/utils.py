# assets/utils.py
import pandas as pd
import numpy as np
import os


def save_to_csv(df: pd.DataFrame, file_name: str) -> None:
    """
    Save a DataFrame to a CSV file.

    :param df: DataFrame to save.
    :param file_name: Base name of the file (without extension).
    """

    directory = os.path.join(os.path.dirname(__file__), "..", "..", "data")

    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{file_name}.csv")
    df.to_csv(file_path, index=False)
