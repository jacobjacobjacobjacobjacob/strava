# assets/utils.py
import pandas as pd
import numpy as np
import os
import logging

logger = logging.getLogger(__name__)


def save_to_csv(df: pd.DataFrame, file_name: str) -> None:
    """
    Save a DataFrame to a CSV file.
    """
    try:
        base_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "strava", "data")
        )

        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
            logger.info(f"Created directory: {base_dir}")

        file_path = os.path.join(base_dir, f"{file_name}.csv")

        # Save DataFrame to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"DataFrame successfully saved to {file_path}")

    except Exception as e:
        logger.error(f"Failed to save DataFrame to CSV: {e}")
        raise
