import os
from uuid import uuid4
from dash import Dash, Output, dcc, html, callback
import dash_bootstrap_components as dbc
from layouts.main import main_layout
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common import utils as cutils

# from layouts.main import main_layout

# Reading and storing configuration

# Stores for important information
app_session_store = dcc.Store(
        id="app-session-store",
        storage_type="session",
        data=dict(
            sid=str(uuid4())
            )
        )

app_schematic_store = dcc.Store(
        id="app-sim-schematic",
        storage_type="session",
        data=dict(
            workloads=list(),
            schedulers=list(),
            actions=list()
        )
)


# Defining the application
app = Dash(__name__,
           compress=True,
           meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])

# Application configuration
app.config.suppress_callback_exceptions = True

# Defining the layout
app.layout = dbc.Container([

    app_session_store,
    app_schematic_store,
    main_layout

], fluid=True, class_name="mh-100", id="layout")


if __name__ == "__main__":
    gui_debug = cutils.envvar_bool_val("ELiSE_GUI_DEBUG")
    # Start application
    app.run(host="0.0.0.0", debug=gui_debug)
