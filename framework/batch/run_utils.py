from datetime import timedelta
from cProfile import Profile
import json
import io
import os
from plotly.io import from_json
import pstats
import sys
from time import time
from types import MethodType
import socket

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common.utils import define_logger, handler_and_formatter, envvar_bool_val, profiling_ctx
logger = define_logger()

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

def patch(evt_logger, extra_features):
    for arg, val in extra_features:
        evt_logger.__dict__[arg] = val
    evt_logger.get_gantt_representation = MethodType(__get_gantt_representation, evt_logger)
    evt_logger.get_workload = MethodType(__get_workload, evt_logger)
    evt_logger.get_animated_cluster = MethodType(__get_animated_cluster, evt_logger)

def pad_message(msg):
    DEFAULT_MSG_LEN = 1024
    return msg + b'\0' * (DEFAULT_MSG_LEN- len(msg))

def single_simulation(sim_batch, server_ipaddr, server_port):
    """The function that defines the simulation loop and actions
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ipaddr, server_port))
    sock.setblocking(False)

    idx, database, cluster, scheduler, evt_logger, compengine, actions, extra_features = sim_batch

    comp_logger = logger.getChild("compengine")
    if envvar_bool_val("ELiSE_DEBUG"):
        handler_and_formatter(comp_logger)
    compengine.debug_logger = comp_logger

    logger.debug(f"Setting up the cluster, scheduler and event logger, (id {idx})")

    cluster.setup()
    scheduler.setup()
    evt_logger.setup()

    # Progress counter
    total_jobs = len(database.preloaded_queue)

    # Start timer
    start_time = time()
    
    with profiling_ctx(idx, scheduler.name, logger):

        while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
            try:
                compengine.sim_step()
            except:
                logger.exception("An error occurred during the execution of the simulation")

            progress_perc = 100 * (1 - (len(database.preloaded_queue) + len(cluster.waiting_queue) + len(cluster.execution_list)) / total_jobs)
            msg_to_send = pad_message(json.dumps( {"id": idx, "progress_perc": progress_perc} ).encode())
            try:
                sock.send(msg_to_send)
            except:
                logger.exception("The socket couldn't connect to the progress server. It will be reconnecting")
                sock.close()
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((server_ipaddr, server_port))
                sock.setblocking(False)
    
    # Calculate the real time and simulated time
    real_time = time() - start_time
    sim_time = cluster.makespan

    # Send the times back to the progress server
    msg_to_send = pad_message(json.dumps( {"id": idx, "scheduler": scheduler.name, "real_time": real_time, "sim_time": sim_time} ).encode())
    sock.send(msg_to_send)

    # Close communication socket
    sock.close()

    # If there are actions provided for this rank
    if actions != []:
        # Overwrite event logger's interface
        extra_features.append(("sim_id", idx))
        patch(evt_logger, extra_features)

        # Perform actions upon completion
        for action in actions:
            getattr(evt_logger, action)()


def multiple_simulations(sim_batches, server_ipaddr, server_port):
    for sim_batch in sim_batches:
        logger.debug(f"Starting single simulation with id {sim_batch[0]}")
        single_simulation(sim_batch, server_ipaddr, server_port)
        logger.debug(f"Finished single simulation with id {sim_batch[0]}")
