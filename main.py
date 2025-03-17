# main.py
import pandas as pd
from loguru import logger
from src.api.strava_api import StravaClient
from src.db.db_manager import DatabaseManager
from src.models.gear import Gear
from src.models.splits import Splits
from src.models.zones import Zones
from src.models.activity import Activity
from src.models.best_efforts import BestEfforts
from src.models.streams import Streams
from src.config import get_strava_config
from src.utils.logger import log_new_activity_details


def main():
    # Retrieves activities
    activities_data = strava_client.get_activities()

    # If no new activites are found
    if not activities_data:
        logger.error("No activities data fetched from Strava.")
        return

    # If data is recieved, turn to dataframe
    activities_df = pd.DataFrame(activities_data)

    # If dataframe is empty, no new activities to process
    if activities_df.empty:
        logger.warning("No new activities to process.")
        db_manager.check_discrepancies()  # Check discrepancies between recieved and stored data
        return

    try:
        # Process and filter activities
        activities_df = Activity.process_activity_data(activities_df)

        # Update gear data
        gear_df = Gear.process_gears(strava_client, activities_df)
        db_manager.insert_dataframe_to_db(df=gear_df, table_name="gear")

        """ SAVE TO CSV """
        # activities_df.to_csv("strava_data.csv", index=False)

        # Retrieve all the cached ids
        cached_ids = db_manager.get_ids_from_cache()

        # Filter the dataframe to only include new activities using the cached ids
        new_activities_df = activities_df[~activities_df["id"].isin(cached_ids)]

        # Extract a list of the new activity IDs
        new_activity_ids = new_activities_df["id"].tolist()

        # If there are new IDs
        if new_activity_ids:
            logger.info(f"{len(new_activity_ids)} new activities.")

            # Insert new activities into the database
            db_manager.insert_dataframe_to_db(
                df=new_activities_df, table_name="activities"
            )

            # Process each new activity in detail
            # logger.debug(f"Cached activities before processing: {len(cached_ids)}")
            process_new_activities(new_activity_ids)

            # len_cache_after = db_manager.get_ids_from_cache()
            # logger.debug(f"Cached activities after processing: {len(len_cache_after)}")
            # logger.debug(
            #     f"New rows inserted to cache: {len(len_cache_after) - len(cached_ids)}"
            # )

        else:
            logger.warning("No new activities.")

    except Exception as e:
        logger.error(f"Error during main processing: {e}")

    finally:
        db_manager.check_discrepancies()


def process_new_activities(new_activity_ids):
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


def process_individual_activity(activity_id, detailed_activity):
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

        # DEBUGGING
        # logger.debug(f"Length of detailed_activity_df: {len(detailed_activity_df)}")
        # logger.debug(f"Length of splits_df: {len(splits_df)}")
        # logger.debug(f"Length of zones_df: {len(zones_df)}")
        # logger.debug(f"Length of best_efforts_df: {len(detailed_activity_df)}")

        # IDs before inserting
        # logger.debug(f"Rows in database before inserting:")
        # splits_before = db_manager.get_ids_from_splits()
        # logger.debug(f"Splits: {len(splits_before)}")
        # streams_before = db_manager.get_ids_from_streams()
        # logger.debug(f"Streams: {len(streams_before)}")
        # best_efforts_before = db_manager.get_ids_from_best_efforts()
        # logger.debug(f"Best Efforts: {len(best_efforts_before)}")
        # zones_before = db_manager.get_ids_from_zones()
        # logger.debug(f"Best Efforts: {len(zones_before)}")

        # Insert processed data into the database
        db_manager.insert_dataframe_to_db(df=splits_df, table_name="splits")
        db_manager.insert_dataframe_to_db(df=zones_df, table_name="zones")
        db_manager.insert_dataframe_to_db(df=best_efforts_df, table_name="best_efforts")
        db_manager.insert_dataframe_to_db(df=streams_df, table_name="streams")

        # COMPARING IDS
        # logger.debug(f"Rows in database after inserting:")
        # splits_after = db_manager.get_ids_from_splits()
        # logger.debug(f"Splits: {len(splits_after)}")
        # streams_after = db_manager.get_ids_from_streams()
        # logger.debug(f"Streams: {len(streams_after)}")
        # best_efforst_after = db_manager.get_ids_from_best_efforts()
        # logger.debug(f"Best Efforts: {len(best_efforst_after)}")
        # zones_after = db_manager.get_ids_from_zones()
        # logger.debug(f"Zones: {len(zones_after)}")

        # DIFFERENCES:
        # logger.debug("Differences in row numbers:")
        # logger.debug(f"Splits: {len(splits_after) - len(splits_before)}")
        # logger.debug(f"Streams: {len(streams_after) - len(streams_before)}")
        # logger.debug(
        #     f"Best Efforts: {len(best_efforst_after) - len(best_efforts_before)}"
        # )
        # logger.debug(f"Zones: {len(zones_after) - len(zones_before)}")

        # logger.success(f"Processed {len(detailed_activity_df)} new activities")

    except Exception as e:
        logger.error(f"Error in processing individual activity {activity_id}: {e}")


if __name__ == "__main__":
    strava_client = StravaClient(**get_strava_config())
    db_manager = DatabaseManager()
    db_manager.create_all_tables()

    # df = db_manager.get_table_as_dataframe("activities")

    main()
