from abc import ABC, abstractmethod
from math import ceil
from itertools import islice

import os
import sys
from typing import Optional

from framework.realsim.compengine import ComputeEngine

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../')
))

from realsim.cluster.host import Host
from realsim.scheduler.scheduler import Scheduler
from realsim.jobs import Job
from typing import Protocol


class ScikitModel(Protocol):
    def predict(self, X):
        pass


class Coscheduler(Scheduler, ABC):
    """Base class for all co-scheduling algorithms
    """

    name = "Abstract Co-Scheduler"
    description = "Abstract base class for all co-scheduling algorithms"

    def __init__(self,
                 backfill_enabled: bool = False,
                 aging_enabled: bool = False,
                 speedup_threshold: float = 1.0,
                 system_utilization: float = 1.0,
                 engine: Optional[ScikitModel] = None):

        Scheduler.__init__(self)

        self.backfill_enabled = backfill_enabled
        self.aging_enabled = aging_enabled
        self.speedup_threshold = speedup_threshold
        self.system_utilization = system_utilization

        self.engine = engine

    @abstractmethod
    def setup(self) -> None:
        pass

    def coloc_condition(self, hostname: str, job: Job) -> float:
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

    def colocation(self, job: Job, socket_conf: tuple) -> bool:
        """We allocate first to the idle hosts and then to the in use hosts
        """

        job.socket_conf = socket_conf
        needed_ppn = sum(job.socket_conf)
        needed_hosts = ceil(job.num_of_processes / needed_ppn)

        #print(socket_conf)

        # Get only the suitable hosts
        suitable_hosts = self.find_suitable_nodes(job.num_of_processes,
                                                  socket_conf)

        # If no suitable hosts where found
        if suitable_hosts == dict():
            return False

        # Apply the colocation condition
        suitable_hosts = dict(
                sorted(suitable_hosts.items(), key=lambda it: self.coloc_condition(it[0], job), reverse=True)
        )

        #self.compeng.deploy_job_to_hosts(islice(suitable_hosts.items(), needed_hosts), job)
        req_hosts = needed_hosts
        req_hosts_psets = list()
        for hostname, psets in suitable_hosts.items():
            req_hosts_psets.append((hostname, psets))
            req_hosts -= 1
            if req_hosts == 0:
                break

        # print(job.get_signature(), socket_conf, req_hosts_psets)
        
        self.compeng.deploy_job_to_hosts(req_hosts_psets, job)

        return True

    @abstractmethod
    def deploy(self) -> bool:
        pass
