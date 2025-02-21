from concurrent.futures import ProcessPoolExecutor
from functools import partial
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator
from common.utils import define_logger
from run_utils import multiple_simulations

logger = define_logger(log_ancestry=True, log_env=True)

batch_creator = BatchCreator(sys.argv[1])
batch_creator.create_ranks()

total_procs = int(sys.argv[2])
batch_size = int(sys.argv[3])
server_ipaddr = sys.argv[4]
server_port = int(sys.argv[5])

logger.debug(f"Creating a process pool of {total_procs} max workers")
executor = ProcessPoolExecutor(max_workers=total_procs)

multiple_simulations_partial = partial(multiple_simulations, server_ipaddr=server_ipaddr, server_port=server_port)

for i in range(total_procs):
    logger.debug(f"Worker {i} gets {batch_size} number of simulation configurations")
    executor.submit(multiple_simulations_partial, batch_creator.ranks[i*batch_size:(i+1)*batch_size])

logger.debug(f"Waiting for the processes to finish")
executor.shutdown(wait=True)
logger.debug(f"The processes have finished without any errors")
