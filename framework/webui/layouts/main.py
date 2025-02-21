from dash import html
import dash_bootstrap_components as dbc

schematic_column = [
        html.H2("Schematics"),
        html.Hr(),
        dbc.Button("Workloads", style={"width": "100%", "margin": "10px 0px"}),
        dbc.Button("Schedulers", style={"width": "100%", "margin": "10px 0px"}),
        dbc.Button("Actions", style={"width": "100%", "margin": "10px 0px"})
        ]

main_layout = dbc.Container([
    dbc.Row([
        dbc.Col(schematic_column, width=2, style={"background-color": "black", "height": "100%"}),
        dbc.Col(width=10, style={"height": "100%"})
    ], align="stretch")
], fluid=True, style={"height": "100%"})
