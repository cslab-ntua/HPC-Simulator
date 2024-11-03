import pymongo
from pymongo.server_api import ServerApi
import importlib

from dash import ClientsideFunction, Input, Output, State, clientside_callback, dcc, html
import dash_bootstrap_components as dbc

import os
import sys

# api
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

# realsim
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../.."
)))

from realsim.generators.abstract import AbstractGenerator

# Get machines, suites per machine and datalogs' names per machine per suite
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
# Get all the documents for datalogs from the database
db = client["storehouse"]
collection  = db["loads"]
documents = [doc["_id"] for doc in collection.find({})]

# Close connection with the database for security reasons
client.close()

# Hold all the information about machines, suites, datalogs and their
# hierarchical relationship
data: dict[str, dict[str, list]] = dict()

# Get all the machines names
machines = list(set([doc["machine"] for doc in documents]))
machines.sort()

# Initialize each machine and suite
for machine in machines:

    # Set an empty dictionary to populate with suites' names
    data[machine] = dict()

    # Get suites' names and sort them alphanumerically
    suites = list(set(
        [doc["suite"] for doc in documents if doc["machine"] == machine]
    ))
    suites.sort()

    # Initialize every suite with empty list for workloads' names

    # A suite that holds all the logs
    data[machine]["All"] = []

    for suite in suites:
        data[machine][suite] = []

# Store all the logs
for doc in documents:
    data[doc["machine"]]["All"].append(doc["load"])
    data[doc["machine"]][doc["suite"]].append(doc["load"])

# Sort all the logs
for _, m_dict in data.items():
    for _, workloads in m_dict.items():
        workloads.sort(key=lambda name: (name.split(".")[0],
                                         int(name.split(".")[2]) ))


# Generators
gen_pack_dir = os.path.join(os.path.dirname(__file__), "../../../realsim/generators")
modules = [
        f"realsim.generators.{module}".replace(".py", "")
        for module in os.listdir(gen_pack_dir)
        if ".py" in module
]

for module in modules:
    importlib.import_module(module)

mapping = dict()
for gen_class in AbstractGenerator.__subclasses__():
    mapping[gen_class.name] = gen_class

gen_names_opt = dbc.Select(
        sorted(list(mapping.keys())), 
        sorted(list(mapping.keys()))[0],
        id="generator-type"
)

elem_generator = dbc.Container([

    dcc.Store(id="generator-store", 
              data=data
              ),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/generator.svg")], width=1),
        dbc.Col([
            dbc.Row([html.P("Workload Generator", className="h5"), html.P("""Select the 
            datalogs from which the jobs will be created and the type of
            generator""")]),
            ])
    ], className="d-flex element-header sticky-top"), 

    # DATALOGS ELEMENT ITEM
    dbc.Row([

        html.Span("Datalogs", className="h6"),

        dbc.Col([ 
                 dbc.Button([
                     html.I(className="bi bi-database",
                            id="datalogs-btn-class")
                ], id="datalogs-btn"),
        ], width=1),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Machine", style={"width": "30%"}),
                dbc.Select(list(data.keys()), 
                           list(data.keys())[0],
                           id="datalogs-machines-select")
                ])
        ], width=6, id="datalogs-machines-display", style={"display": "block"}),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Suite", style={"width": "30%"}),
                dbc.Select(list(data[list(data.keys())[0]]), 
                           "All",
                           id="datalogs-suites-select")
                ])
        ], width=5, id="datalogs-suites-display", style={"display": "block"}),
        
        dbc.Col([
            dbc.Button("Upload directory", id="datalogs-upload-directory",
                       style={"width": "100%"})
        ], width=11, id="datalogs-upload-display", style={"display": "none"})


    ], class_name="element-item m-1 p-2"),

    # GENERATOR ELEMENT ITEM
    dbc.Row([
        html.Span("Trace options", className="h6"),
        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Generator"),
                gen_names_opt
            ])
        ]),
        # dbc.Col([
        #     dbc.InputGroup([
        #         dbc.InputGroupText("Trace type", style={"width": "50%"}),
        #         dbc.Select(["Static", "Dynamic"], "Static", id='simulation-type')
        #     ]),
        #     dbc.InputGroup([
        #         dbc.InputGroupText("Distribution", style={"width": "50%"}),
        #         dbc.Select(["Constant", "Poisson", "Random"], "Constant",
        #                    id='simulation-distribution')
        #     ], id="simulation-distribution-display", style={"display": "none"}),

        #     dbc.Col([
        #         dbc.InputGroup([
        #             dbc.InputGroupText("Generator interval (sec)", style={"width": "50%"}),
        #             dbc.Input(value=0, type="number", min=0, 
        #                       id="generator-time")
        #         ]),
        #     ], id="generator-time-display", style={"display": "none"}),

        # ])
    ], class_name="element-item m-1 p-2"),

    # GENERATOR OPTIONS ELEMENT ITEM
    dbc.Row([
        html.Span("Generator Options", className="h6"),
        dbc.Spinner([

            dbc.Container(id="generator-options", style={"overflow": "scroll"})

                    ], color="primary")
        ], class_name="element-item m-1 p-2 d-flex align-self-strech"),

    dcc.Upload(id="upload-tracefile")

    ], class_name="element d-flex align-items-strech")

# Clientside callback for datalogs element-item management
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="datalogs_events"
        ),
        Output("datalogs-btn-class", "className"),
        Output("datalogs-machines-select", "value"),
        Output("datalogs-suites-select", "options"),
        Output("datalogs-suites-select", "value"),
        Output("datalogs-machines-display", "style"),
        Output("datalogs-suites-display", "style"),
        Output("datalogs-upload-display", "style"),

        Input("datalogs-btn", "n_clicks"),
        Input("datalogs-machines-select", "value"),
        State("datalogs-suites-select", "options"),
        State("datalogs-suites-select", "value"),
        State("generator-store", "data")
)

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="generator_options"
        ),
        Output("generator-options", "children"),

        Input("generator-type", "value"),
        Input("datalogs-machines-select", "value"),
        Input("datalogs-suites-select", "value"),
        State("generator-store", "data")
)
