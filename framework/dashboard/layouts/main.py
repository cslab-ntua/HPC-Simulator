from dash import MATCH, ClientsideFunction, Input, Output, State, clientside_callback, dcc, html, callback
import dash_bootstrap_components as dbc
import dash_loading_spinners as dls

from layouts.elements.generator import elem_generator
from layouts.elements.cluster import elem_cluster
from layouts.elements.schedulers import elem_schedulers
from layouts.elements.run import elem_run

main_data = {"figures": dict(), "list of jobs": dict(), "runs": 0}

main_layout = dbc.Container([

    # STORE
    dcc.Store(id="main-store", data=main_data, storage_type="memory"),
    dcc.Download(id='download-workload'),

    # EXPORT FIGURES MODAL
    dbc.Modal([
        dbc.ModalHeader([dbc.ModalTitle("Save figures")]),
        dbc.ModalBody([
            dbc.Row([
                dbc.InputGroup([
                    dbc.InputGroupText("Height", style={"width": "25%"}),
                    dbc.Input(type="number", min=1, value=1, id="figures-height"),
                    dbc.InputGroupText("px")
                    ], class_name="my-2")
                ]),
            dbc.Row([
                dbc.InputGroup([
                    dbc.InputGroupText("Width", style={"width": "25%"}),
                    dbc.Input(type="number", min=1, value=1, id="figures-width"),
                    dbc.InputGroupText("px")
                    ], class_name="my-2")
                ]),
            dbc.Row([
                dbc.Select(["jpg", "png", "svg", "pdf", "html", "json"], "jpg",
                           id="figures-format")
                ]),
            dbc.Row([
                dcc.Download(id="download-figures"),
                dbc.Button("Save figures", id="save-figures-btn",
                           class_name="my-2",
                           style={"width": "60%"})
                ], justify="center")
            ])
        ], id="export-figures-modal", is_open=False, centered=True),


    # RESULTS
    # dbc.Spinner([
    dls.GridFade([
        dbc.Modal([

            dbc.Container([
                dbc.Button(">", id="results-nav-btn", style={"borderRadius": 0}),
                dbc.Button([html.I(className="bi bi-download",
                                   id="results-download-graph")], 
                           color="secondary", style={"borderRadius": 0})
            ], style={
                "margin": 0,
                "padding": 0,
                "position": "fixed",
                "top": 0,
                "zIndex": 100,
                }),

            dbc.Button("x", id="results-close-btn",
                       href="#",
                       color="danger",
                       style={"position": "fixed", 
                              "top": 0, 
                              "right": 0,
                              "zIndex": 100,
                              "borderRadius": 0}),

            dbc.ModalBody([

                dcc.Graph(id="results-graph",
                          style={"width": "100%", "height": "100%"})

            ])
            ], is_open=False, id="results-modal", fullscreen=True)

    ], fullscreen=True, color="#3584E4", radius=10),

    dbc.Offcanvas(id="results-nav", 
                  title="Select figure",
                  is_open=False, 
                  style={"z-index": "2000", 
                         "background-color": "rgba(255, 255, 255, 0.9)"}),

    dcc.Location(id="results-graph-select"),

    # DASHBOARD
    dbc.Row([
        dbc.Col([ elem_generator ], class_name="py-3"),
        dbc.Col([ elem_cluster ], class_name="p-3 d-flex align-items-strech align-self-strech"),
        ], className="d-flex align-items-center justify-content-center"),

    dbc.Row([
        dbc.Col([ elem_schedulers ], class_name="p-3"),
        dbc.Col([ elem_run ])
        ], class_name="d-flex align-items-center justify-content-center align-self-strech")

    ], class_name="flex-row h-100", fluid=True, style={"height": "100vh"})

# Stylize results screen/modal when figure navigation is open
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="results_nav_isopen"
        ),
        Output("results-modal", "style"),
        Input("results-nav", "is_open"),
        prevent_initial_call=True
)

# Callback to open/close the results screen/modal
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="results_modal_isopen"
        ),
        Output("results-modal", "is_open"),
        Input("results-close-btn", "n_clicks"),
        Input("simulation-results-btn", "n_clicks"),
        prevent_initial_call=True
)

# Open navigation to select figures
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="open_results_nav"
        ),

        Output("results-nav", "is_open"),
        Input("results-nav-btn", "n_clicks"),

        prevent_initial_call=True

)

# Open collapse for the matched experiment by index
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="open_experiment_collapse"
        ),

        Output({"type": "results-exp-collapse", "index": MATCH}, "is_open"),
        Input({"type": "results-exp-btn", "index": MATCH}, "n_clicks"),

        prevent_initial_call=True

)

# Create a plot
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="create_graph"
        ),

        Output("results-graph", "figure"),
        Input("results-graph-select", "hash"),
        State("results-store", "data"),

        prevent_initial_call=True

)

# Download graphs/plots
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="results_download_graph"
        ),
        Output("results-download-graph", "n_clicks"),
        Input("results-download-graph", "n_clicks"),
        prevent_initial_call=True
)

# Download workload
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="results_download_workload"
        ),

        Output("download-workload", "data"),
        Input("results-graph-select", "hash"),
        State("results-store", "data"),

        prevent_initial_call=True

)
