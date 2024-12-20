import subprocess
import os
import sys
import argparse
from multiprocessing import cpu_count

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provide a project file and a parallelizing provider to run simulations")
    parser.add_argument("-f", "--project-file", help="Provide a project file name", required=True)
    parser.add_argument("-p", "--provider", choices=["mpi", "mp"], default="mp", help="Define the provider for parallelizing tasks")
    args = parser.parse_args()

    project_file = args.project_file
    provider = args.provider

    if provider == "mp":
        print("Using python's multiprocessing as backend")

        process = subprocess.Popen(["python", "run_mp.py", project_file])

    elif provider == "mpi":
        print("Using MPI as backend")

        from batch.batch_utils import BatchCreator

        batch_creator = BatchCreator(project_file)
        sims_num = batch_creator.get_procs_num()

        # print(f"Number of sims = {sims_num}")
        #INFO: in a hpc system we need the total for all the hosts allocated
        # so for example in SLURM we need the env variable SLURM_TASKS
        total_threads = cpu_count()
        if sims_num < total_threads:
            total_threads = sims_num

        process = subprocess.Popen(["mpirun", "--bind-to", "none", "--oversubscribe", "-np", str(total_threads), "python", "run_mpi.py", project_file])

    else:
        raise RuntimeError("No appropriate provider was given")
