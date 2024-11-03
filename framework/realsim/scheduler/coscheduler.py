from abc import ABC, abstractmethod

import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../')
))

from realsim.scheduler.scheduler import Scheduler
from realsim.jobs import Job



class Coscheduler(Scheduler, ABC):
    """Base class for all co-scheduling algorithms
    """

    name = "Abstract Co-Scheduler"
    description = "Abstract base class for all co-scheduling algorithms"

    def __init__(self):
        Scheduler.__init__(self)

    @abstractmethod
    def setup(self) -> None:
        pass

    def host_alloc_condition(self, hostname: str, job: Job) -> float:
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """

        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If no signatures then spread
        if co_job_sigs == []:
            return job.max_speedup

        # Get the worst possible speedup
        first_co_job_name = co_job_sigs[0].split(":")[-1]
        worst_speedup = self.database.heatmap[job.job_name][first_co_job_name]
        worst_speedup = worst_speedup if worst_speedup is not None else 1

        for co_job_sig in co_job_sigs[1:]:
            co_job_name = co_job_sig.split(":")[-1]
            speedup = self.database.heatmap[job.job_name][co_job_name]
            speedup = speedup if speedup is not None else 1
            if speedup < worst_speedup:
                worst_speedup = speedup

        return worst_speedup

    @abstractmethod
    def deploy(self) -> bool:
        pass
