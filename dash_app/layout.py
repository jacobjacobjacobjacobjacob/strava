# dash_app/layout.py
from dash import Dash, html, dcc
import pandas as pd

from dash_app.components import dropdowns
from dash_app.components.plots.bar_chart import render as render_bar_chart


def create_layout(app: Dash, data: pd.DataFrame) -> html.Div:
    """
    Creates the main layout of the Dash application.

    Args:
        app (Dash): The Dash application instance.
        data (pd.DataFrame): The data used for the dashboard.

    Returns:
        html.Div: The layout of the Dash application.
    """
    return html.Div(
        className="app-div",
        children=[
            dcc.Location(id="url", refresh=False),
            html.Div(id="page-content"),
        ],
    )


def render_main_dashboard(app: Dash, data: pd.DataFrame) -> html.Div:
    """
    Renders the main dashboard layout with charts and dropdowns.

    Args:
        app (Dash): The Dash application instance.
        data (pd.DataFrame): The data used for the dashboard.

    Returns:
        html.Div: The layout of the main dashboard.
    """

    bar_chart_metrics = ["distance", "duration", "elevation"]
    metric_charts = [
        render_bar_chart(app, data, metric, f"bar-chart-{metric}", chart_type="metric")
        for metric in bar_chart_metrics
    ]
    activity_chart = render_bar_chart(
        app, data, "activities", "bar-chart-activities", chart_type="count"
    )

    # fmt: off
    return html.Div(
        className="app-div",
        children=[
            html.H1(app.title),
            html.Hr(),
            html.Div(
                className="dropdown_container",
                children=[
                    dropdowns.year_dropdown.render(app, data),
                    dropdowns.month_dropdown.render(app, data),
                    dropdowns.sport_type_dropdown.render(app, data),
                ],
            ),
            html.Div(
                className="chart_container",
                children=metric_charts + [activity_chart],
                style={"display": "flex", "flexDirection": "row", "flexWrap": "wrap"},
            ),
        ],
    )
    # fmt: on


def render_year_dashboard(app: Dash, data: pd.DataFrame) -> html.Div:
    return html.Div(
        children=[
            html.H1("Another Page"),
            html.P("This is another page content."),
            dcc.Link("Go to Home", href="/"),
        ]
    )
