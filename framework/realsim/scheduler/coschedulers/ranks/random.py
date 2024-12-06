from .ranks import RanksCoscheduler
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class RandomRanksCoscheduler(RanksCoscheduler):

    name = "Random Ranks Co-Scheduler"
    description = """Random co-scheduling using ranks architecture as a fallback
    to classic scheduling algorithms"""

    def host_alloc_condition(self, hostname: str, job: Job) -> float:
        return float(self.cluster.hosts[hostname].state != Host.IDLE)

    def backfill(self) -> bool:
        return False
