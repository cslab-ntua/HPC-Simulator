from mpi4py import MPI
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

def run_sim(input_data):

    (idx, database, cluster, scheduler, logger, compengine, config) = input_data

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    #logger.get_gantt_representation().show()

    # Handle CSV action
    for action in config["actions"]:
        print(action)
        if action["type"] == "export-csv":
            _csv = os.path.join(action["output_dir"], f'{rank}_{scheduler.name}.csv')
            os.makedirs(action["output_dir"], exist_ok=True)
            with open (_csv,'w') as _f:
                _f.write(logger.get_workload())

    print(f"Rank{rank} finished")



if rank == 0:
    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    for i, sim_batch in enumerate(batch_creator.ranks[1:]):
        comm.send(sim_batch, dest=i+1, tag=22)

    print("Sent pickled objects")

    input_data = batch_creator.ranks[0]
    run_sim(input_data)


else:
    input_data = comm.recv(source=0, tag=22)
    run_sim(input_data)
