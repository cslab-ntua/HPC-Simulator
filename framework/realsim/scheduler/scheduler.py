from __future__ import annotations
from abc import ABC, abstractmethod
import os
import sys
from functools import reduce
from itertools import islice
from typing import TYPE_CHECKING
from math import ceil

from procset import ProcSet

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from realsim.jobs import Job
from realsim.database import Database
from realsim.cluster.cluster import Cluster
from realsim.logger.logger import Logger
from realsim.compengine import ComputeEngine

class Scheduler(ABC):
    """Scheduler provides a base class for scheduling methods. When creating an
    instance of Cluster a scheduler must always be provided and a reference to
    the cluster should also be provided to the scheduler instance. This way both
    instances can communicate their data immediately.
    """

    # The name of the scheduling algorithm
    name = "Abstract Scheduler"

    # Describe the philosophy of the scheduler
    description = "The abstract base class for all scheduling algorithms"

    def __init__(self, backfill_enabled: bool = False):

        self.database: Database
        self.cluster: Cluster
        self.logger: Logger
        self.compeng: ComputeEngine

        # Variable to test whether a backfill policy is enabled
        self.queue_depth = None # None for the whole waiting queue
        self.backfill_enabled: bool = backfill_enabled
        self.backfill_depth: int = 100
        self.aging_enabled: bool = False
        self.age_threshold: int = 10
        self.time_step = 30 # Decide every 30 seconds 
        self.timer = self.time_step

    def find_suitable_nodes(self, 
                            req_cores: int, 
                            socket_conf: tuple) -> dict[str, list[ProcSet]]:
        """ Returns hosts and their procsets that a job can use as resources
        + req_cores   : required cores for the job
        + socket_conf : under a certain socket mapping/configuration
        """
        cores_per_host = sum(socket_conf)
        to_be_allocated = dict()
        for hostname, host in self.cluster.hosts.items():
            # If under the specifications of the required cores and socket 
            # allocation
            if reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, host.sockets))):
                req_cores -= cores_per_host
                to_be_allocated.update({hostname: [
                    ProcSet.from_str(' '.join([str(x) for x in p_set[:socket_conf[i]]]))
                    for i, p_set in enumerate(host.sockets)]
                })

        # If the amount of cores needed is covered then return the list of possible
        # hosts
        if req_cores <= 0:
            return to_be_allocated
        # Else, if not all the cores can be allocated return an empty list
        else:
            return {}

    def host_alloc_condition(self, hostname: str, job: Job) -> float:
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """
        return 1.0

    def allocation(self, job: Job, socket_conf: tuple) -> bool:
        """We allocate first to the idle hosts and then to the in use hosts
        """
        """Co-allocate a job to an executing job under a specific socket mapping/configuration
        + job: the job we want to co-allocate
        + socket_conf: socket mapping/configuration for the job
        """

        job.socket_conf = socket_conf

        # Get only the suitable hosts
        suitable_hosts = self.find_suitable_nodes(job.num_of_processes,
                                                  socket_conf)

        # If no suitable hosts where found
        if suitable_hosts == dict():
            return False

        # Apply the colocation condition
        suitable_hosts = dict(
                sorted(suitable_hosts.items(), key=lambda it: self.host_alloc_condition(it[0], job), reverse=True)
        )
        needed_ppn = sum(job.socket_conf)
        needed_hosts = ceil(job.num_of_processes / needed_ppn)

        req_hosts = needed_hosts
        req_hosts_psets = list()
        for hostname, psets in suitable_hosts.items():
            req_hosts_psets.append((hostname, psets))
            req_hosts -= 1
            if req_hosts == 0:
                break

        self.compeng.deploy_job_to_hosts(req_hosts_psets, job)

        return True

    def compact_allocation(self, job: Job) -> bool:
        return self.allocation(job, self.cluster.full_socket_allocation)

    def pop(self, queue: list[Job]) -> Job:
        """Get and remove an object from a queue
        """
        obj = queue[0]
        queue.remove(obj)
        return obj

    @abstractmethod
    def setup(self) -> None:
        """Basic setup of scheduling algorithm before the start of the
        simulation
        """
        pass

    def backfill(self) -> bool:
        """A backfill algorithm for the scheduler
        """
        return False
        
    def waiting_queue_reorder(self, job: Job) -> float:
        """Waiting queue reordering logic
        """
        return 1.0

    @abstractmethod
    def deploy(self) -> bool:
        """Abstract method to deploy the new execution list to the cluster
        """
        pass
