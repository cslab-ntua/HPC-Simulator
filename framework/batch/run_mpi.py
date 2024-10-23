from mpi4py import MPI
import os
import sys
from time import time
from datetime import timedelta

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from run_utils import simulation

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    for i, sim_batch in enumerate(batch_creator.ranks[1:]):
        comm.send(sim_batch, dest=i+1, tag=22)

    print("Sent pickled objects")

    # Calculate the time it took to finish the simulation
    start_time = time()
    # Execute the simulation
    simulation(batch_creator.ranks[0])

    end_time = time()

    print(f"Rank{rank} finished with time = {timedelta(seconds=(end_time - start_time))}")

else:

    # Calculate the time it took to finish the simulation
    start_time = time()

    # Execute the simulation
    simulation(comm.recv(source=0, tag=22))

    end_time = time()

    print(f"Rank{rank} finished with time = {timedelta(seconds=(end_time - start_time))}")
