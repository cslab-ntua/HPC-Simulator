from math import ceil
import subprocess
import os
import sys
import argparse
from batch_utils import BatchCreator
from multiprocessing import cpu_count

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

def local_or_hpc_env():
    if "SLURM_TASKS" in os.environ:
        return int(os.environ["SLURM_TASKS"])
    return cpu_count()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provide a project file and a parallelizing provider to run simulations")
    parser.add_argument("-f", "--project-file", help="Provide a project file name", required=True)
    parser.add_argument("-p", "--provider", choices=["mpi", "mp"], default="mp", help="Define the provider for parallelizing tasks")
    args = parser.parse_args()

    project_file = args.project_file
    provider = args.provider

    # Calculate the number of needed cores to run all the simulations in parallel
    batch_creator = BatchCreator(project_file)
    sims_num = batch_creator.get_procs_num()
    avail_cores = local_or_hpc_env()

    if avail_cores >= sims_num:
        total_procs = sims_num
        queue_size = 1
    else:
        total_procs = avail_cores
        queue_size = ceil(sims_num / avail_cores)

    if provider == "mp":
        print("Using python's multiprocessing as backend")

        process = subprocess.Popen(["python", "run_mp.py", project_file, str(total_procs), str(queue_size)], env=os.environ.copy())

    elif provider == "mpi":
        print("Using MPI as backend")

        process = subprocess.Popen(["mpirun", "--bind-to", "none", "--oversubscribe", "-np", str(total_procs), "python", "run_mpi.py", project_file, str(queue_size)], env=os.environ.copy())

    else:
        raise RuntimeError("No appropriate provider was given")
