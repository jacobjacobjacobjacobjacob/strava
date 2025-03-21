# src/utils/config.py
import os


# Default coordinates for your location to fall back to if no coordinates are found
DEFAULT_COORDINATES = "63.43086170118938, 10.390790858381667"

DATABASE_NAME = "database.db"
DATABASE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "database", DATABASE_NAME
)

""" Apple Health Data Config"""
ENABLE_APPLE_HEALTH_DATA = True
HEALTH_DATA_CSV_NAME = "health_export_data.csv"
HEALTH_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "database", "health_data"
)
PATH_TO_APPLE_HEALTH_DATA = os.path.join(HEALTH_DATA_DIR, HEALTH_DATA_CSV_NAME)
