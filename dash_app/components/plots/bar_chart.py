# plots/bar_chart.py
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

from dash_app.components.ids import *
from dash_app.components.schema import DataSchema
from assets.utils import metric_formats, all_months


def render(
    app: Dash, data: pd.DataFrame, metric: str, chart_id: str, chart_type: str
) -> html.Div:

    @app.callback(
        Output(chart_id, "children"),
        [
            Input(YEAR_DROPDOWN, "value"),
            Input(MONTH_DROPDOWN, "value"),
            Input(SPORT_TYPE_DROPDOWN, "value"),
        ],
    )
    def update_bar_chart(
        years: list[str],
        months: list[str],
        sport_type: list[str],
    ) -> html.Div:

        filtered_data = data.query(
            "year in @years and month in @months and sport_type in @sport_type"
        )

        if filtered_data.empty:
            return html.Div("No data selected.")

        if chart_type == "count":
            # Number of activities chart
            num_activities = (
                filtered_data.groupby(DataSchema.MONTH).size().reset_index(name="count")
            )
            fig_count = px.bar(
                num_activities,
                x=DataSchema.MONTH,
                y="count",
                title="",
                text="count",
            )
            fig_count.update_traces(texttemplate="%{text}", textposition="inside")
            fig_count.update_layout(
                title={
                    "text": "Activities",
                    "x": 0.5,  # Center title horizontally
                    "xanchor": "center",  # Center title relative to x
                },
                xaxis_title="Month",
                yaxis_title="",
                xaxis={
                    "categoryorder": "array",
                    "categoryarray": all_months,
                },  # Ensure months are in the right order
            )
            return html.Div([dcc.Graph(figure=fig_count)])

        # Handle regular metrics
        if metric == "":
            return html.Div("Error: Metric is not specified.")

        if DataSchema.MONTH not in filtered_data.columns:
            return html.Div(f"Error: '{DataSchema.MONTH}' column not found in data.")

        if metric not in filtered_data.columns:
            return html.Div(f"Error: '{metric}' column not found in data.")

        try:
            aggregated_data = filtered_data.groupby(DataSchema.MONTH, as_index=False)[
                [metric]
            ].sum()
        except KeyError as e:
            return html.Div(f"Error: Column not found - {str(e)}")

        fig_metric = px.bar(
            aggregated_data,
            x=DataSchema.MONTH,
            y=metric,
            text=metric,
        )
        fig_metric.update_traces(texttemplate="%{text:.2s}", textposition="inside")

        fig_metric.update_layout(
            title={
                "text": f"{metric.capitalize()} ({metric_formats.get(metric, '')})",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="",
            yaxis_title="",
            xaxis={
                "categoryorder": "array",
                "categoryarray": all_months,
            },
        )

        return html.Div([dcc.Graph(figure=fig_metric)])

    return html.Div(id=chart_id)
