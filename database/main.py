# database/main.py.py
import sqlalchemy
import pandas as pd
import logging
import sqlite3

from assets.config import setup_logging

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


engine = sqlalchemy.create_engine("sqlite:///database/activities.db")


def write_to_database(df: pd.DataFrame, table_name: str) -> None:
    """
    Writes the DataFrame to the specified table in the SQLite database.

    :param df: DataFrame to be written to the database.
    :param table_name: Name of the table in the SQLite database.

    """
    if df.empty:
        logger.warning("The DataFrame is empty. Nothing to write to the database.")
        return

    try:
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame head:\n{df.head()}")

        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    except Exception as e:
        logger.error(
            f"An error occurred while writing to the database: {e}", exc_info=True
        )
