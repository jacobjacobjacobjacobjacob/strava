# dropdowns/month_dropdown.py
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import pandas as pd

from assets.utils import all_months
from dash_app.components.ids import *
from dash_app.components.schema import DataSchema


def render(app: Dash, data: pd.DataFrame) -> html.Div:
    # Extract unique months from data
    all_data_months = data[DataSchema.MONTH].dropna().unique().tolist()
    unique_months = sorted(
        set(all_data_months),
        key=lambda month: (
            all_months.index(month) if month in all_months else len(all_months)
        ),
    )

    # Set default values as a sorted list
    default_values = unique_months

    @app.callback(
        Output(MONTH_DROPDOWN, "value"),
        [
            Input(YEAR_DROPDOWN, "value"),
            Input(SELECT_ALL_MONTHS_BUTTON, "n_clicks"),
        ],
    )
    def update_months(years: list[str], _: int) -> list[str]:
        if years:
            filtered_data = data.query("year in @years")
            filtered_months = sorted(
                set(filtered_data[DataSchema.MONTH].dropna().unique()),
                key=lambda month: (
                    all_months.index(month) if month in all_months else len(all_months)
                ),
            )
        else:
            filtered_months = default_values  # Use sorted months if no filter applied
        return filtered_months

    # fmt: off
    return html.Div(
        children=[
            html.H6("Month"),
            dcc.Dropdown(
                id=MONTH_DROPDOWN,
                options=[{"label": month, "value": month} for month in unique_months],
                value=default_values,  # Ensure default values are sorted
                multi=True,
            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=SELECT_ALL_MONTHS_BUTTON,
                n_clicks=0,
            ),
        ]
    )
    # fmt: on
