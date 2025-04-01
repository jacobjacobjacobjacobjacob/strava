# main.py
import pandas as pd
from loguru import logger
from src.api.strava_api.strava_api import StravaClient
from src.api.config import get_strava_api_config
from src.db.db_manager import DatabaseManager
from src.models.gear import Gear
from src.models.splits import Splits
from src.models.zones import Zones
from src.models.activity import Activity
from src.models.best_efforts import BestEfforts
from src.models.streams import Streams
from src.models.health import AppleHealth
from src.config import (
    ENABLE_APPLE_HEALTH_DATA,
    PATH_TO_APPLE_HEALTH_DATA,
)
from src.utils.logging import (
    log_new_activity_details,
    log_apple_health_data_toggle,
    log_new_activities_count,
    log_new_gear,
)


def main():
    # Retrieves activities
    activities_data = strava_client.get_activities()


    # If no new activites are found
    if not activities_data:
        logger.error("Could not fetch data from Strava.")
        return

    # If data is recieved, turn to dataframe
    activities_df = pd.DataFrame(activities_data)

    # If dataframe is empty, no new activities to process
    if activities_df.empty:
        logger.warning("No new activities to process.")
        db_manager.check_strava_database_discrepancies()
        return

    try:
        """ACTIVITY DATA"""
        # Process and filter activities
        logger.info("Processing activity data...")
        activities_df = Activity.process_activity_data(activities_df)

        # Retrieve all the cached ids
        cached_ids = db_manager.get_ids_from_cache()


        # Filter the dataframe for new activities and get a list of new activity IDs
        new_activities_df = activities_df[~activities_df["id"].isin(cached_ids)]

        new_activity_ids = new_activities_df["id"].tolist()

        # If there are new IDs
        if new_activity_ids:
            log_new_activities_count(new_activity_ids)

            process_new_activities(new_activity_ids, new_activities_df)
            process_gear_data(df=activities_df)

        else:
            logger.info("No new activities.\n")

    except Exception as e:
        logger.error(f"Error during main processing: {e}")

    finally:
        db_manager.check_strava_database_discrepancies()

    """ APPLE HEALTH DATA 
    # Health data processing
    if not ENABLE_APPLE_HEALTH_DATA:
        log_apple_health_data_toggle()
        return
    try:
        # Process Apple Health data if enabled
        log_apple_health_data_toggle()
        process_apple_health_data()
    except Exception as e:
        logger.error(f"Error processing Apple Health data: {e}")

    finally:
        if ENABLE_APPLE_HEALTH_DATA:
            db_manager.check_health_database_discrepancies()"""


def process_gear_data(df: pd.DataFrame) -> None:
    gear_ids = db_manager.get_gear_ids()
    gear_df = Gear.process_gears(strava_client, df)
    new_gear_df = gear_df[~gear_df["gear_id"].isin(gear_ids)]

    if not new_gear_df.empty:
        log_new_gear(new_gear_df)

    db_manager.insert_dataframe_to_db(df=gear_df, table_name="gear")


def process_new_activities(new_activity_ids, new_activities_df):
    db_manager.insert_dataframe_to_db(df=new_activities_df, table_name="activities")
    for activity_id in new_activity_ids:
        db_manager.update_cache(activity_id)

        try:
            detailed_activity = strava_client.get_detailed_activity(activity_id)
            logger.debug(f"Processing activity id: {activity_id}")

            if not detailed_activity:
                logger.warning(f"Activity {activity_id} has no detailed data.")
                continue

            # Process the detailed activity data if it exists
            process_individual_activity(activity_id, detailed_activity)

        except Exception as e:
            logger.error(f"Error processing activity {activity_id}: {e}")


def process_individual_activity(activity_id: int, detailed_activity):
    try:
        detailed_activity_df = pd.DataFrame([detailed_activity])
        splits_df = Splits.process_splits(strava_client, detailed_activity_df)
        zones_data = strava_client.get_activity_zones(activity_id)
        zones_df = Zones.process_zones(zones_data, activity_id)
        best_efforts_data = detailed_activity.get("best_efforts", [])
        best_efforts_df = BestEfforts.process_best_efforts(
            activity_id, best_efforts_data
        )
        streams_df = Streams.get_streams(strava_client, activity_id)

        log_new_activity_details(activity_id, detailed_activity_df)

        # Insert processed data into the database
        db_manager.insert_dataframe_to_db(df=splits_df, table_name="splits")
        db_manager.insert_dataframe_to_db(df=zones_df, table_name="zones")
        db_manager.insert_dataframe_to_db(df=best_efforts_df, table_name="best_efforts")
        db_manager.insert_dataframe_to_db(df=streams_df, table_name="streams")

    except Exception as e:
        logger.error(f"Error in processing individual activity {activity_id}: {e}")


def process_apple_health_data():
    """Handles the Apple Health data processing."""
    health_df = AppleHealth.from_csv(PATH_TO_APPLE_HEALTH_DATA)
    # logger.debug(f"Original HEALTH DF shape: {health_df.shape}")

    # Crop df
    # logger.debug(f"Cropped HEALTH DF shape: {health_df.shape}")

    # Unique dates in the new DF
    # log_info(f"Unique dates in the new DF: {len(health_df['date'].unique())}")

    # Unique dates in the DB
    dates = db_manager.get_dates_from_health()
    # log_info(f"Unique dates in the DB: {len(dates)}")

    # Dates in the dataframe that are not in the DB
    # logger.debug(f"Dates in the dataframe that are not in the DB: {health_df[~health_df['date'].isin(dates)]['date'].unique()}")

    # Filter the dataframe to only include new health data
    new_health_data = health_df[~health_df["date"].isin(dates)]
    # logger.debug(f"Number of new dates: {len(new_health_data['date'].tolist())}")

    # Insert health data into the database
    if len(new_health_data) == 0:
        logger.info("No new health data.")
    else:
        logger.debug(f"Inserting {len(new_health_data)} new rows into Database...")
        db_manager.insert_dataframe_to_db(df=new_health_data, table_name="health")


if __name__ == "__main__":
    # logger.error("MOVE ALL ERROR HANDLING TO SEPARATE FILE")

    strava_client = StravaClient(**get_strava_api_config())

    db_manager = DatabaseManager()
    # db_manager.drop_table("activities")
    db_manager.create_all_tables()
    # db_manager.delete_last_activity()
    # df = db_manager.get_table_as_dataframe("activities")

    # run_df = df[df["sport_type"] == "Run"]

    


    main()

    def log_database_summary():
        logger.info("Make something here to log a summary of all records in database")
        logger.info("Log project info in general")

    # Filtering Operations
    # from rich import Console
    # from rich import Table
    # console = Console()
    



    # def print_dataframe(df: pd.DataFrame, title: str = "DataFrame", highlight_columns=None):
    #     """Prints a Pandas DataFrame as a styled Rich table.
        
    #     Args:
    #         df (pd.DataFrame): The DataFrame to print.
    #         title (str): Table title.
    #         highlight_columns (list): Columns to highlight in bold.
    #     """
    #     if df.empty:
    #         console.print(f"[bold red]{title} is empty![/bold red]")
    #         return

    #     table = Table(title=title)

    #     # Highlight specific columns if provided
    #     highlight_columns = set(highlight_columns or [])

    #     # Add column headers
    #     for col in df.columns:
    #         style = "bold yellow" if col in highlight_columns else "cyan"
    #         table.add_column(str(col), justify="right", style=style)

    #     # Add rows
    #     for _, row in df.iterrows():
    #         table.add_row(*map(str, row))

    #     console.print(table)
