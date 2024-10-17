from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class RandomRanksCoscheduler(RanksCoscheduler, ABC):

    name = "Random Ranks Co-Scheduler"
    description = """Random co-scheduling using ranks architecture as a fallback
    to classic scheduling algorithms"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return job.num_of_processes

    def coloc_condition(self, hostname: str, job: Job) -> float:
        return float(self.cluster.hosts[hostname].state == Host.IDLE)

    def backfill(self) -> bool:

        deployed = False

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        # Ascending sorting by their wall time
        backfilling_jobs.sort(key=lambda b_job: b_job.wall_time)

        for b_job in backfilling_jobs:

            # Colocate
            if self.colocation(b_job, self.cluster.half_socket_allocation):
                deployed = True
                self.after_deployment()
            # Compact
            # elif super().compact_allocation(b_job):
            #     deployed = True
            #     self.after_deployment()
            else:
                break

        return deployed
