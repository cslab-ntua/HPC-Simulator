from mpi4py import MPI
import os
import sys
from time import time
from datetime import timedelta
from cProfile import Profile
import pstats

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import import_module
from run_utils import simulation

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:

    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    for i, sim_batch in enumerate(batch_creator.ranks[1:]):
        # Send the necessary modules to import first
        comm.send(batch_creator.mods_export, dest=i+1, tag=10)
        # Then, send the simulation batch
        comm.send(sim_batch, dest=i+1, tag=22)

    # Calculate the time it took to finish the simulation
    start_time = time()

    # profiler = Profile()
    # profiler.enable()

    # Execute the simulation
    simulation(batch_creator.ranks[0])

    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats("cumtime")
    # stats.print_stats(30)


    end_time = time()

    print(f"Rank{rank} finished with time = {timedelta(seconds=(end_time - start_time))}")

else:

    # Import all the necessary modules before starting the simulation
    necessary_modules = comm.recv(source=0, tag=10)
    for mod in necessary_modules:
        import_module(mod)

    # Calculate the time it took to finish the simulation
    start_time = time()

    # Execute the simulation
    simulation(comm.recv(source=0, tag=22))

    end_time = time()

    print(f"Rank{rank} finished with time = {timedelta(seconds=(end_time - start_time))}")
