# dropdowns/year_dropdown.py
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import pandas as pd

from dash_app.components.ids import *
from dash_app.components.schema import DataSchema


def render(app: Dash, data: pd.DataFrame) -> html.Div:
    all_years: list[str] = data[DataSchema.YEAR].tolist()
    unique_years = sorted(set(all_years), key=int)

    @app.callback(
        Output(YEAR_DROPDOWN, "value"),
        Input(SELECT_ALL_YEARS_BUTTON, "n_clicks"),
    )
    def select_all_years(_: int) -> list[str]:
        return unique_years

    # fmt: off
    return html.Div(
        children=[
            html.H6("Year"),
            dcc.Dropdown(
                id=YEAR_DROPDOWN,
                options=[
                    {"label": year, "value": year}
                    for year in unique_years
                ],
                value=unique_years,
                multi=True,

            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=SELECT_ALL_YEARS_BUTTON
            )
        ]
    )
    # fmt: on
