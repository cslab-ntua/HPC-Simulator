import os
from plotly.io import from_json
from types import MethodType

def __get_gantt_representation(self):
    res = self.__class__.get_gantt_representation(self) # Have to call this way to avoid infinite recursion
    fig = from_json(res)
    fig.update_layout(width=2048, height=1024)

    output_path = os.path.abspath(f"{self.img_dir}")
    os.makedirs(output_path, exist_ok=True)
    fig.write_image(f"{output_path}/workload_{self.sim_id}_{self.scheduler.name.lower().replace(' ', '_')}.png")

def __get_workload(self):
    res = self.__class__.get_workload(self)

    output_path = os.path.abspath(f"{self.workload_dir}")
    os.makedirs(output_path, exist_ok=True)

    with open(f"{output_path}/workload_{self.sim_id}_{self.scheduler.name.lower().replace(' ', '_')}.csv", "w") as fd:
        fd.write(res)

def __get_animated_cluster(self):
    res = self.__class__.get_animated_cluster(self)
    fig = from_json(res)
    fig.show()

def patch(logger, extra_features):
    for arg, val in extra_features:
        logger.__dict__[arg] = val
    logger.get_gantt_representation = MethodType(__get_gantt_representation, logger)
    logger.get_workload = MethodType(__get_workload, logger)
    logger.get_animated_cluster = MethodType(__get_animated_cluster, logger)


def simulation(sim_batch):
    """The function that defines the simulation loop and actions
    """
    idx, database, cluster, scheduler, logger, compengine, actions, extra_features = sim_batch

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    # If there are actions provided for this rank
    if actions != []:
        # Overwrite logger's interface
        extra_features.append(("sim_id", idx))
        patch(logger, extra_features)

        # Perform actions upon completion
        for action in actions:
            getattr(logger, action)()

