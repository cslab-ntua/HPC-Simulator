from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
from math import inf
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class JungleCoscheduler(RanksCoscheduler, ABC):

    name = "Jungle Co-Scheduler"
    description = """Co-scheduling using ranks architecture to favor job avg speeedup"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return 1.0

    def coloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """

        # get the jobs signatures that are assinged to the host
        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If no signatures then spread
        if co_job_sigs == []:
            return (job.max_speedup, inf)

        # get average speedup for each job in host + candidate job
        speedup = list(map(lambda j: self.database.heatmap[job.job_name][j.split(":")[-1]],co_job_sigs))
        speedup += list(map(lambda j: self.database.heatmap[j.split(":")[-1]][job.job_name],co_job_sigs))
        avg_speedup = sum(speedup) / (len(co_job_sigs)*2)

        # get how many of the speedup values of pairs job,x and x,job for each x in host are bellow threshold
        jobs_with_speedup = list(filter(lambda j: self.database.heatmap[job.job_name][j.split(":")[-1]] >= 1,co_job_sigs))
        jobs_with_speedup += list(filter(lambda j: self.database.heatmap[j.split(":")[-1]][job.job_name] >= 1,co_job_sigs))
        speedup_counts = len(jobs_with_speedup)

        return (avg_speedup, speedup_counts)
