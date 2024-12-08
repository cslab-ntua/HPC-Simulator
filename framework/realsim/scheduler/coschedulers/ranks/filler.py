import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class FillerCoscheduler(RanksCoscheduler):

    name = "Filler Co-Scheduler"
    description = """Co-scheduler that tries to fill the ''holes'' 
    in the HPC system's resources created by the allocation of jobs inside"""

    # def host_alloc_condition(self, hostname, job):
    #     worst_speedup = super().host_alloc_condition(hostname, job)
    #     idx = int(hostname.replace("host", "")) + 1
    #     return worst_speedup/idx


    # def host_alloc_condition(self, hostname: str, job: Job) -> float:
    #     return float(self.cluster.hosts[hostname].state != Host.IDLE)
    
    def waiting_queue_reorder(self, job: Job) -> float:
        # The job that is closer to cover the gaps is more preferrable
        sys_free_cores = self.cluster.get_idle_cores()
        if sys_free_cores > 0:
            diff = sys_free_cores - job.num_of_processes
            if diff > 0:
                factor0 = 1 - (diff/sys_free_cores)
            elif diff == 0:
                factor0 = 1
            else:
                factor0 = -1
        else:
            factor0 = 1

        factor1 = ((job.job_id + 1) / len(self.cluster.waiting_queue))

        return factor0 / factor1

    # def coloc_condition(self, hostname: str, job: Job) -> float:
    #     return float(self.cluster.hosts[hostname].state != Host.IDLE)
