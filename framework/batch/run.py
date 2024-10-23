import os
import sys
import multiprocessing as mp

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

def run_sim(input_data):

    (idx, database, cluster, scheduler, logger, compengine, config) = input_data

    cluster.setup()
    scheduler.setup()
    logger.setup()

    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        compengine.sim_step()

    # Handle CSV action
    for action in config["actions"]:
        print(action)
        if action["type"] == "export-csv":
            _csv = os.path.join(action["output_dir"], f'{idx}_{scheduler.name}.csv')
            os.makedirs(action["output_dir"], exist_ok=True)
            with open(_csv, 'w') as _f:
                _f.write(logger.get_workload())

    print(f"Process {idx} finished")


def worker(input_data):
    run_sim(input_data)


if __name__ == "__main__":
    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    # Use multiprocessing pool to parallelize the simulation execution
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(worker, batch_creator.ranks)

    print("Simulation batches sent to workers.")
