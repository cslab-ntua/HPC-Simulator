import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))

from realsim.scheduler.scheduler import Scheduler
from realsim.jobs.utils import deepcopy_list


class FIFOScheduler(Scheduler):

    name = "FIFO Scheduler"
    description = "First In First Out/ First Come First Served scheduling policy"

    def __init__(self):
        Scheduler.__init__(self)

    def setup(self):
        pass

    def deploy(self) -> bool:

        deployed = False
        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])

        while waiting_queue != []:

            job = self.pop(waiting_queue)
            if self.compact_allocation(job):
                deployed = True
            else:
                break

        return deployed

