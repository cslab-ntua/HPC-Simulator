from dash import ClientsideFunction, Input, Output, clientside_callback, dcc, html
import dash_bootstrap_components as dbc
import pymongo
from pymongo.server_api import ServerApi

# Automatic selection from machine names
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
db = client["storehouse"]
collection  = db["machines"]
documents = [doc for doc in collection.find({})]
client.close()

# Sort documents by machine name
documents.sort(key=lambda doc: doc["_id"])

# Create cluster store
cluster_store: dict[str, dict] = dict()
for doc in documents:
    cluster_store[doc["_id"]] = {
            "nodes": int(doc["nodes"]),
            "ppn": int(doc["ppn"])
    }
machines = list(cluster_store.keys())

elem_cluster = dbc.Container([

    dcc.Store(id="cluster-store", data=cluster_store),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/cluster.svg")], width=1),
        dbc.Col([
            dbc.Row([html.H5("Cluster"),
                     html.P("""Select by name or manually the specifications of
                            the cluster.""")])
            ], width=10)
    ], class_name="element-header sticky-top"),

    dbc.Row([

        html.Span("Number of nodes and cores", className="h6"),

        dbc.Row([
            dbc.Button("To manual configuration", id="cluster-btn")
            ], class_name="p-3"),

        dbc.Row([
            dbc.Select(machines, machines[0], id="cluster-machines")
        ], id="cluster-names-display", style={"display": "block"},
                class_name="mx-2"),

        dbc.Row([
            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText("nodes", style={"width": "50%"}),
                    dbc.Input(type="number", min=0, value=0, id="cluster-nodes")
                ])
            ], style={"display": "inline-block"}, width=6),

            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText("ppn", style={"width": "50%"}),
                    dbc.Input(type="number", min=0, value=0, id="cluster-ppn")
                ])
            ], style={"display": "inline-block"}, width=6),

        ], id="cluster-manual-display", style={"display": "none"}, class_name="mx-2"),
    ], class_name="element-item m-1 p-2"),

    dbc.Row([
        html.Span("Queue Options", className="h6"),
        dbc.InputGroup([
            dbc.InputGroupText("Waiting queue size", style={"width": "50%"}),
            dbc.Input(min=-1, value=-1, type="number", id="queue-size")
        ])
    ], class_name="element-item m-1 p-2")


], class_name="element d-flex align-items-strech")

clientside_callback(

        ClientsideFunction(
            namespace="clientside",
            function_name="cluster_events"
        ),

        Output("cluster-btn", "children"),
        Output("cluster-names-display", "style"),
        Output("cluster-manual-display", "style"),

        Input("cluster-btn", "n_clicks"),

)
