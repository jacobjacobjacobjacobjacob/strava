# dropdowns/sport_type_dropdown.py
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import pandas as pd

from dash_app.components.ids import *
from dash_app.components.schema import DataSchema


def get_combined_options(data: pd.DataFrame) -> list[dict]:
    # Generate dropdown options from the combined_type column
    unique_options = sorted(data["sport_type"].dropna().unique())
    combined_options = [{"label": opt, "value": opt} for opt in unique_options]
    return combined_options


def render(app: Dash, data: pd.DataFrame) -> html.Div:
    # Generate initial dropdown options based on the combined_type column
    dropdown_options = get_combined_options(data)
    default_values = [
        opt["value"]
        for opt in dropdown_options
        if opt["value"].startswith("bike")  # Default to all bike-related values
    ]

    @app.callback(
        Output(SPORT_TYPE_DROPDOWN, "options"),
        [
            Input(YEAR_DROPDOWN, "value"),
            Input(SELECT_ALL_SPORT_TYPES_BUTTON, "n_clicks"),
        ],
    )
    def update_sport_types(years: list[str], _: int) -> list[dict]:
        if years:
            filtered_data = data.query("year in @years")
            filtered_options = get_combined_options(filtered_data)
        else:
            filtered_options = (
                dropdown_options  # No filtering applied if no years selected
            )
        return filtered_options

    @app.callback(
        Output(SPORT_TYPE_DROPDOWN, "value"),
        Input(SELECT_ALL_SPORT_TYPES_BUTTON, "n_clicks"),
    )
    def select_all_sports(n_clicks: int) -> list[str]:
        if n_clicks:
            return [
                opt["value"] for opt in get_combined_options(data)
            ]  # Return all options
        return default_values

    # fmt: off
    return html.Div(
        children=[
            html.H6("Sport Type"),
            dcc.Dropdown(
                id=SPORT_TYPE_DROPDOWN,
                options=dropdown_options,
                value=default_values,
                multi=True,
            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=SELECT_ALL_SPORT_TYPES_BUTTON,
                n_clicks=0,
            ),
        ]
    )
    # fmt: on
