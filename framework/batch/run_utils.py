from datetime import timedelta
from cProfile import Profile
import io
import os
from plotly.io import from_json
import pstats
import sys
from time import time
from types import MethodType

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

# Get the environment for this process
workingdir = os.environ.get("ELiSE_WORKINGDIR", ".")
ELiSE_Report = os.environ.get("ELiSE_REPORT", 1)
ELiSE_Progress = os.environ.get("ELiSE_PROGRESS", None)
ELiSE_Time = os.environ.get("ELiSE_TIME", None)
ELiSE_Profiling = os.environ.get("ELiSE_PROFILING", None)

from common.utils import define_logger, handler_and_formatter
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


def single_simulation(sim_batch):
    """The function that defines the simulation loop and actions
    """

    idx, database, cluster, scheduler, evt_logger, compengine, actions, extra_features = sim_batch

    comp_logger = logger.getChild("compengine")
    handler_and_formatter(comp_logger)
    compengine.debug_logger = comp_logger

    logger.debug(f"Setting up the cluster, scheduler and event logger, (id {idx})")

    cluster.setup()
    scheduler.setup()
    evt_logger.setup()

    #TODO: make profiling and timer a context environment

    # Progress segment
    total_jobs = len(database.preloaded_queue)

    # Profiling segment
    profiler = Profile()
    if ELiSE_Profiling:
        logger.debug("Profiling is enabled")
        profiler.enable()

    # Timing segment
    start_time = time()

    if ELiSE_Progress:
        print(f"\rTotal progress: 0.00%", end="")
        logger.debug("Progress reports are enabled")
    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        try:
            compengine.sim_step()
            if ELiSE_Progress:
                print(f"\rTotal progress: {100 * (1 - (len(database.preloaded_queue) + len(cluster.waiting_queue) + len(cluster.execution_list)) / total_jobs):.2f}%", end="")
        except:
            logger.exception("An error occurred during the execution of the simulation")

    # Timing segment
    if ELiSE_Time:
        end_time = time()
        print(f"{scheduler.name} took {end_time - start_time} s to finish")

    # Profiling segment
    if ELiSE_Profiling:
        profiler.disable()
        strstream = io.StringIO()
        stats = pstats.Stats(profiler, stream=strstream).sort_stats("cumtime")
        stats.print_stats(30)
        os.makedirs(f"{workingdir}/reports", exist_ok=True)
        with open(f"{workingdir}/reports/workload_{idx}_{scheduler.name.lower().replace(' ', '_')}_profile.log") as fd:
            fd.write(strstream.getvalue())

    if ELiSE_Report:
        real_time = time() - start_time
        sim_time = cluster.makespan
        reportlines = list()
        reportlines.append(f"Real time = {timedelta(seconds=real_time)}")
        reportlines.append(f"Simulated time = {timedelta(seconds=sim_time)}")
        reportlines.append(f"Time ratio = {sim_time / (24 * real_time)} sim days / 1 real hr")
        try:
            os.makedirs(f"{workingdir}/reports", exist_ok=True)
            with open(f"{workingdir}/reports/workload_{idx}_{scheduler.name.lower().replace(' ', '_')}_report.log", "w") as fd:
                fd.write("\n".join(reportlines))
        except:
            raise RuntimeError("Couldn't write report")

    # If there are actions provided for this rank
    if actions != []:
        # Overwrite event logger's interface
        extra_features.append(("sim_id", idx))
        patch(evt_logger, extra_features)

        # Perform actions upon completion
        for action in actions:
            getattr(evt_logger, action)()


def multiple_simulations(sim_batches):
    for sim_batch in sim_batches:
        logger.debug(f"Starting single simulation with id {sim_batch[0]}")
        single_simulation(sim_batch)
        logger.debug(f"Finished single simulation with id {sim_batch[0]}")
