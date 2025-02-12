import argparse
from batch_utils import BatchCreator
from math import ceil
from multiprocessing import cpu_count
import os
import sys
import subprocess

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common.utils import define_logger
logger = define_logger()

def local_or_hpc_env():
    logger.debug("Checking if we are inside a scheduler environment")

    total_cores = -1

    if "SLURM_TASKS" in os.environ:
        logger.debug("Inside a SLURM environment")
        total_cores = int(os.environ["SLURM_TASKS"])
    else:
        logger.debug("Not in a scheduler environment. Executing in localhost")
        total_cores = cpu_count()

    if total_cores <= 0:
        logger.exception(f"The total amount of available cores is {total_cores} <= 0")
        raise RuntimeError(f"The total amount of available cores is {total_cores} <= 0")
    else:
        logger.debug(f"The total amount of available cores is {total_cores}")

    return total_cores

def calculate_for_less_avail_cores(sim_configs_num, avail_cores):
    batch_size = ceil(sim_configs_num / avail_cores)
    total_procs = 0
    while sim_configs_num > 0:
        sim_configs_num -= batch_size
        total_procs += 1

    return total_procs, batch_size

if __name__ == "__main__":

    supported_providers = ["mpi", "mp"]

    parser = argparse.ArgumentParser(description="Provide a project file and a parallelizing provider to run simulations")
    parser.add_argument("-f", "--project-file", help="Provide a project file name", required=True)
    parser.add_argument("-p", "--provider", choices=supported_providers, default="mp", help="Define the provider for parallelizing tasks")
    args = parser.parse_args()

    project_file = args.project_file
    provider = args.provider

    # Calculate the number of needed cores to run all the simulations in parallel
    batch_creator = BatchCreator(project_file)
    
    sim_configs_num = batch_creator.get_sim_configs_num()
    logger.debug(f"The total number of simulation configurations is {sim_configs_num}")

    # Calculate the number of available cores under the context
    avail_cores = local_or_hpc_env()

    if avail_cores >= sim_configs_num:
        total_procs = sim_configs_num
        batch_size = 1
    else:
        total_procs, batch_size = calculate_for_less_avail_cores(sim_configs_num, avail_cores)

    total_procs_str = f"One process" if total_procs == 1 else f"{total_procs} parallel processes"
    batch_str = f"a single simulation configuration" if batch_size == 1 else f"{batch_size} simulation configurations"
    logger.debug(f"{total_procs_str} for {batch_str}")

    submission_cmd = list()
    if provider == "mp":
        logger.debug("Using Python's multiprocessing library as backend")
        submission_cmd = ["python", "run_mp.py", project_file, str(total_procs), str(batch_size)]

    elif provider == "mpi":
        logger.debug("Using MPI as backend")
        submission_cmd = ["mpirun", "--bind-to", "none", "--oversubscribe", "-np", str(total_procs), "python", "run_mpi.py", project_file, str(batch_size)]

    logger.debug(f"Submission command: {' '.join(submission_cmd)}")
    process = subprocess.Popen(submission_cmd, env=os.environ.copy())
