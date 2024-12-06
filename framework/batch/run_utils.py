from datetime import timedelta
import os
import io
from time import time
from cProfile import Profile
import pstats
from plotly.io import from_json
from types import MethodType

# Get the environment for this process
workingdir = os.environ.get("ELiSE_WORKINGDIR", ".")
ELiSE_Report = os.environ.get("ELiSE_REPORT", 1)
ELiSE_Progress = os.environ.get("ELiSE_PROGRESS", None)
ELiSE_Time = os.environ.get("ELiSE_TIME", None)
ELiSE_Profiling = os.environ.get("ELiSE_PROFILING", None)


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


def single_simulation(sim_batch):
    """The function that defines the simulation loop and actions
    """

    idx, database, cluster, scheduler, logger, compengine, actions, extra_features = sim_batch

    cluster.setup()
    scheduler.setup()
    logger.setup()

    #TODO: make profiling and timer a context environment

    # Progress segment
    total_jobs = len(database.preloaded_queue)

    # Profiling segment
    profiler = Profile()
    if ELiSE_Profiling:
        profiler.enable()

    # Timing segment
    start_time = time()

    if ELiSE_Progress:
        print(f"\rTotal progress: 0.00%", end="")
    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()
        if ELiSE_Progress:
            print(f"\rTotal progress: {100 * (1 - (len(database.preloaded_queue) + len(cluster.waiting_queue) + len(cluster.execution_list)) / total_jobs):.2f}%", end="")
    print()

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
        # Overwrite logger's interface
        extra_features.append(("sim_id", idx))
        patch(logger, extra_features)

        # Perform actions upon completion
        for action in actions:
            getattr(logger, action)()


def multiple_simulations(sim_batches):
    for sim_batch in sim_batches:
        single_simulation(sim_batch)
