# dropdowns/day_of_week_dropdown.py
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import pandas as pd

from assets.utils import all_weekdays
from dash_app.components.ids import *
from dash_app.components.schema import DataSchema

all_weekdays = [day.capitalize() for day in all_weekdays]


def render(app: Dash, data: pd.DataFrame) -> html.Div:
    @app.callback(
        Output(DAY_OF_WEEK_DROPDOWN, "value"),
        Input(SELECT_ALL_DAYS_OF_WEEK_BUTTON, "n_clicks"),
    )
    def select_all_days(_: int) -> list[str]:
        return all_weekdays

    # fmt: off
    return html.Div(
        children=[
            html.H6("Day"),
            dcc.Dropdown(
                id=DAY_OF_WEEK_DROPDOWN,
                options=[{"label": day, "value": day} for day in all_weekdays],
                value=all_weekdays,
                multi=True,
            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=SELECT_ALL_DAYS_OF_WEEK_BUTTON,
                n_clicks=0,
            ),
        ]
    )
    # fmt: on
