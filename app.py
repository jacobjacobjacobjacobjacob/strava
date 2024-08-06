# app.py

from dash import Dash, Output, Input
from dash_bootstrap_components.themes import BOOTSTRAP

from dash_app.layout import (
    create_layout,
    render_main_dashboard,
    render_year_dashboard,
)
from dash_app.data_loader import load_strava_data

from assets.config import DASHAPP_TITLE, STRAVA_DATA_PATH

# STRAVA_DATA_PATH = "/Users/jacob/Desktop/py/strava/data/clean_data.csv"
# DASHAPP_TITLE = "Strava"


def main() -> None:
    data = load_strava_data(STRAVA_DATA_PATH)
    app = Dash(__name__, external_stylesheets=[BOOTSTRAP])

    app.title = DASHAPP_TITLE
    app.layout = create_layout(app, data)

    # Callback to handle page content rendering based on URL
    @app.callback(Output("page-content", "children"), [Input("url", "pathname")])
    def display_page(pathname):
        if pathname == "/":
            return render_main_dashboard(app, data)
        elif pathname == "/year":
            return render_year_dashboard(app)
        else:
            return html.Div(
                [html.H1("404: Not found"), html.P("This page does not exist.")]
            )

    app.run(debug=True)


if __name__ == "__main__":
    main()
