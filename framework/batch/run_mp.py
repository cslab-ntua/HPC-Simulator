from concurrent.futures import ProcessPoolExecutor
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator
from run_utils import multiple_simulations

batch_creator = BatchCreator(sys.argv[1])
batch_creator.create_ranks()

total_procs = int(sys.argv[2])
queue_size = int(sys.argv[3])
executor = ProcessPoolExecutor(max_workers=total_procs)

for i in range(total_procs):
    executor.submit(multiple_simulations, batch_creator.ranks[i*queue_size:(i+1)*queue_size])

executor.shutdown(wait=True)
