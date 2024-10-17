import os
from plotly.io import from_json
from types import MethodType

def __get_gantt_representation(self):
    res = self.__class__.get_gantt_representation(self) # Have to call this way to avoid infinite recursion
    fig = from_json(res)
    fig.update_layout(width=1024, height=780)
    if not os.path.exists("./images"):
        os.mkdir("./images")

    fig.write_image(f"./images/workload_{self.sim_id}_{self.scheduler.name.lower().replace(' ', '_')}.png")

def __get_workload(self):
    res = self.__class__.get_workload(self)

    if not os.path.exists("./csvs"):
        os.mkdir("./csvs")

    with open(f"./csvs/workload_{self.sim_id}_{self.scheduler.name.lower().replace(' ', '_')}.csv", "w") as fd:
        fd.write(res)

def patch(logger):
    logger.get_gantt_representation = MethodType(__get_gantt_representation, logger)
    logger.get_workload = MethodType(__get_workload, logger)


def simulation(sim_batch):
    """The function that defines the simulation loop and actions
    """
    idx, database, cluster, scheduler, logger, compengine, actions = sim_batch

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    # Overwrite logger's interface
    logger.sim_id = idx
    patch(logger)

    # Perform actions upon completion
    for action in actions:
        getattr(logger, action)()

