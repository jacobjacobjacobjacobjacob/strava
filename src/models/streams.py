# src/models/streams.py
import pandas as pd
import json
from loguru import logger


class Streams:
    def __init__(self, strava_client):
        self.strava_client = strava_client

    ALL_STREAM_TYPES = [
        "time",
        "distance",
        "latlng",
        "altitude",
        "velocity_smooth",
        "heartrate",
        "cadence",
        "watts",
    ]

    @staticmethod
    def process_streams(activity_id, response) -> pd.DataFrame:
        """Process and serialize streams data."""
        streams_data = []

        keys = [
            "time",
            "distance",
            "latlng",
            "altitude",
            "velocity_smooth",
            "heartrate",
            "cadence",
            "watts",
        ]

        # Dictionary to hold stream data for the activity
        row_data = {"id": activity_id}

        for key in keys:
            stream_values = response.get(key, {}).get("data", None)

            # Serialize the list as JSON if it exists
            row_data[key] = (
                json.dumps(stream_values) if stream_values else json.dumps([0])
            )

        # Rename `velocity_smooth` to `speed`
        row_data["speed"] = row_data.pop("velocity_smooth", json.dumps([0]))

        streams_data.append(row_data)

        columns = [
            "id",
            "time",
            "distance",
            "latlng",
            "altitude",
            "speed",
            "heartrate",
            "cadence",
            "watts",
        ]
        streams_df = pd.DataFrame(streams_data, columns=columns)

        return streams_df

    @staticmethod
    def get_streams(
        strava_client,
        activity_id,
        keys=ALL_STREAM_TYPES,
        resolution="medium",
        key_by_type=True,
    ) -> pd.DataFrame:
        params = {
            "keys": ",".join(keys) if keys else None,
            "resolution": resolution,
            "key_by_type": str(key_by_type).lower(),
        }
        try:
            streams_response = strava_client.make_request(
                f"activities/{activity_id}/streams", params=params
            )

            if not streams_response:
                logger.info(
                    f"No stream data found for activity {activity_id}, skipping."
                )
                return pd.DataFrame()

            streams_df = Streams.process_streams(activity_id, streams_response)
            return streams_df

        except Exception as e:
            logger.error(f"Error fetching streams for activity {activity_id}: {e}")
            return pd.DataFrame()
