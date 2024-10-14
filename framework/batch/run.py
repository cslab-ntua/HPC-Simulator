from mpi4py import MPI
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    for i, sim_batch in enumerate(batch_creator.ranks[1:]):
        comm.send(sim_batch, dest=i+1, tag=22)

    print("Sent pickled objects")

    idx, database, cluster, scheduler, logger, compengine = batch_creator.ranks[0]

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    #logger.get_gantt_representation().show()
    print(f"Rank{rank} finished")

else:

    idx, database, cluster, scheduler, logger, compengine = comm.recv(source=0, tag=22)

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    #logger.get_gantt_representation().show()
    print(f"Rank{rank} finished")
