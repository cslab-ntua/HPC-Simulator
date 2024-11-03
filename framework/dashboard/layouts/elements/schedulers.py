from dash import MATCH, Output, Input, State, dcc, html, callback, clientside_callback, ClientsideFunction
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from .updateschedulers import update_schedulers
from inspect import signature


stored_modules: dict[str, dict] = dict()

@callback(
        Output(component_id="schedulers-store", component_property="data"),
        State(component_id="schedulers-store", component_property="data"),
        Input(component_id="update-schedulers", component_property="n_intervals")
        )
def CB_update_schedulers(store_data, n_intervals):

    # If there is a new scheduling python module then re-issue the names of the
    # algorithms
    if update_schedulers(stored_modules) or (store_data == [] and stored_modules != dict()):

        data = list()

        for mod_name, mod_dict in stored_modules.items():

            if mod_dict["viewable"]:

                # Hyperparameters name and type
                hyperparams: dict[str, str] = dict()
                for name, param in signature(mod_dict["classobj"]).parameters.items():
                    hyperparams[name] = str(param.annotation)

                if "FIFO" in mod_dict["classobj"].name:
                    data.insert(0, {
                        "module": mod_name,
                        "name": mod_dict["classobj"].name,
                        "description": mod_dict["classobj"].description,
                        "selected": True,
                        "disabled": False,
                        "hyperparams": hyperparams
                    })

                elif "EASY" in mod_dict["classobj"].name:
                    data.insert(0, {
                        "module": mod_name,
                        "name": mod_dict["classobj"].name,
                        "description": mod_dict["classobj"].description,
                        "selected": True,
                        "disabled": False,
                        "hyperparams": hyperparams
                    })
                
                elif "Random" in mod_dict["classobj"].name:

                    if data != [] and "Default" in data[0]["name"]:
                        data.insert(1, {
                            "module": mod_name,
                            "name": mod_dict["classobj"].name,
                            "description": mod_dict["classobj"].description,
                            "selected": True,
                            "disabled": False,
                            "hyperparams": hyperparams
                        })

                    else:
                        data.insert(0, {
                            "module": mod_name,
                            "name": mod_dict["classobj"].name,
                            "description": mod_dict["classobj"].description,
                            "selected": True,
                            "disabled": False,
                            "hyperparams": hyperparams
                        })

                else:
                    # Check if the scheduler existed in the previous data stored
                    # and if holds true check whether it was selected
                    index = None
                    for i, sched_dict in enumerate(store_data):
                        if mod_dict["classobj"].name in sched_dict:
                            index = i
                            break

                    data.append({
                        "module": mod_name,
                        "name": mod_dict["classobj"].name,
                        "description": mod_dict["classobj"].description,
                        "selected": store_data[index] if index is not None else False,
                        "disabled": False,
                        "hyperparams": hyperparams
                    })

        return data

    else:

        # If nothing changed or there was a code change then only the backend
        # will deal with the changes
        raise PreventUpdate

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="create_schedulers_checklist"
        ),
        Output("schedulers-container", "children"),
        Input("schedulers-store", "data")
)

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="show_scheduler_hyperparameters"
        ),
        Output({'type': 'schedulers-collapse', 'index': MATCH}, 'is_open'),
        Input({'type': 'schedulers-checkboxes', 'index': MATCH}, 'value')
)


elem_schedulers = dbc.Container([

    dcc.Interval(id="update-schedulers", interval=10000),

    dcc.Store(id="schedulers-store", data=list()),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/scheduler.svg")], width=1),
        dbc.Col([
            dbc.Row([html.H5("Schedulers"), 
                     html.P("""Select which scheduling algorithms will
                            participate in the simulation.""")])
            ], width=10)
    ], class_name="element-header sticky-top"),

    dbc.Container(id="schedulers-container", class_name="py-1")
], class_name="element")
