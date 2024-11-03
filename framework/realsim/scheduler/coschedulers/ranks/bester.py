from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
import os
import sys
from math import inf

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class BesterCoscheduler(RanksCoscheduler, ABC):

    name = "Bester Ranks Co-Scheduler"
    description = """Random co-scheduling using ranks architecture as a fallback
    to classic scheduling algorithms"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return 1.0

    def coloc_condition(self, hostname: str, job: Job) -> tuple:

        # Get all the executing jobs in the host
        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If there are not then the execution will be spread and we want to
        # promote this
        if co_job_sigs == []:
            return (inf, inf)

        co_job = None
        for xjob in self.cluster.execution_list:
            if xjob.get_signature() == co_job_sigs[0]:
                co_job = xjob

        # This is a guard
        if co_job is None:
            raise RuntimeError("No job found in the execution list")

        # If the job and co-job need the same amount of nodes then it decreases
        # the fragmentation so we should promote this
        points = 0

        if self.cluster.get_idle_cores() > 0.25 * self.cluster.get_used_cores():
            if job.half_socket_nodes >= co_job.half_socket_nodes/2:
                points += 1

        # If the estimated co-run time is roughly the same and they both have
        # good avg speedup then promote
        sp1 = self.database.heatmap[job.job_name][co_job.job_name]
        sp2 = self.database.heatmap[co_job.job_name][job.job_name]
        if sp1 is None or sp2 is None:
            return (points, job.avg_speedup)

        avg_sp = (sp1 + sp2) / 2

        estimated_rem_time = (co_job.start_time + co_job.wall_time) - self.cluster.makespan
        if abs(job.wall_time - estimated_rem_time) / estimated_rem_time < 0.2 and avg_sp >= 1:
            points += 1

        return (points, avg_sp)

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
