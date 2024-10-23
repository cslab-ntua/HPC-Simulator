from concurrent.futures import ProcessPoolExecutor
import os
import sys
from time import time
from datetime import timedelta

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator
from run_utils import simulation

batch_creator = BatchCreator(sys.argv[1])
batch_creator.create_ranks()

# Create the multiprocessing executor
executor = ProcessPoolExecutor()
for sim_batch in batch_creator.ranks:
    executor.submit(simulation, sim_batch)
executor.shutdown(wait=True)
