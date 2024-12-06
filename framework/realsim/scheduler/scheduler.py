from __future__ import annotations
from abc import ABC, abstractmethod
import os
import sys
from functools import partial, reduce
from itertools import islice
from typing import TYPE_CHECKING, Optional
from math import ceil

from concurrent.futures import ProcessPoolExecutor, wait
from multiprocessing import Manager, cpu_count

from procset import ProcSet

realsim_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../"
        )
    )
sys.path.append(realsim_path)
# os.environ["PATH"] = ":" + realsim_path
# os.environ["PYTHONPATH"] = realsim_path

from realsim.jobs import Job
from realsim.database import Database
from realsim.cluster.cluster import Cluster
from realsim.cluster.host import Host
from realsim.logger.logger import Logger
from realsim.compengine import ComputeEngine

# import ray
# ray.init()
# 
# 
# @ray.remote
def par_find_suitable_nodes_func(socket_conf, names_and_hosts, chunk_size, i):
    suitable_hosts = dict()
    cores_per_host = sum(socket_conf)
    total_cores = 0

    # for hostname, host in names_and_hosts[i*chunk_size:(i+1)*chunk_size]:
    for name_procs in names_and_hosts[i*chunk_size:(i+1)*chunk_size]:
        hostname = name_procs[0]
        procs = [ProcSet.from_str(proc) for proc in name_procs[1:]]
        # If under the specifications of the required cores and socket 
        # allocation *takes advantage of short circuit for idle hosts
        if reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, procs))):
            total_cores += cores_per_host
            suitable_hosts.update({hostname: [
                ProcSet.from_str(' '.join([str(x) for x in p_set[:socket_conf[i]]]))
                for i, p_set in enumerate(procs)]
            })

    return suitable_hosts, total_cores

class Scheduler(ABC):
    """Scheduler class is the abstract base class for all the scheduling
    algorithms that feed the simulation. It provides basic utility methods
    such as searching for hosts with enough resources, allocation and 
    allocation conditionals that are concrete for any inherited class. It
    also provides abstract methods for deploying and backfilling that need
    to be defined in concrete schedulers that are used in the simulation.
    """

    # The name of the scheduling algorithm
    name = "Abstract Scheduler"

    # Describe the philosophy of the scheduler
    description = "The abstract base class for all scheduling algorithms"
    def __init__(self):

        # References
        self.database: Database
        self.cluster: Cluster
        self.logger: Logger
        self.compeng: ComputeEngine

        # Properties of the scheduling algorithms
        self.queue_depth = None # None is equivalent to using the whole waiting queue
        self.backfill_enabled: bool = True # The most basic algorithm will not use backfill
        self.backfill_depth = 100 # How far we reach for backfilling

    def oldest_find_suitable_nodes(self, 
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
            # allocation *takes advantage of short circuit for idle hosts
            if host.state == Host.IDLE or reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, host.sockets))):
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

    def find_suitable_nodes(self, 
                            req_cores: int, 
                            socket_conf: tuple,
                            immediate=False):
        """ Returns hosts and their procsets that a job can use as resources
        + req_cores   : required cores for the job
        + socket_conf : under a certain socket mapping/configuration
        """
        cores_per_host = sum(socket_conf)
        to_be_allocated = dict()
        for hostname, host in self.cluster.hosts.items():
            # If under the specifications of the required cores and socket 
            # allocation *takes advantage of short circuit for idle hosts
            if host.state == Host.IDLE or reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, host.sockets))):
                req_cores -= cores_per_host
                to_be_allocated.update({hostname: [
                    ProcSet.from_str(' '.join([str(x) for x in p_set[:socket_conf[i]]]))
                    for i, p_set in enumerate(host.sockets)]
                })
                if immediate:
                    if req_cores <= 0:
                        return to_be_allocated, True

        return to_be_allocated, req_cores <= 0

    def old_find_suitable_nodes(self, 
                            req_cores: int, 
                            socket_conf: tuple,
                            immediate=False) -> tuple[dict, bool]:

        socket_conf_ref = ray.put(socket_conf)
        suitable_hosts_ref = ray.put(self.cluster.get_hostname_procs())

        futures = ray.get([par_find_suitable_nodes_func.remote(socket_conf_ref, suitable_hosts_ref, self.chunk_size_ref, i) for i in range(self.max_workers)])
        suitable_nodes = dict()
        for hostprocs, total_cores in futures:
            suitable_nodes.update(hostprocs)
            req_cores -= total_cores

        return suitable_nodes, req_cores <= 0


    # def find_suitable_nodes_first_par(self, 
    #                         req_cores: int, 
    #                         socket_conf: tuple):
    #     """ Returns hosts and their procsets that a job can use as resources
    #     + req_cores   : required cores for the job
    #     + socket_conf : under a certain socket mapping/configuration
    #     """

    #     #print("FSN: 0")

    #     self.mgr_req_cores.set(req_cores)
    #     self.mgr_suitable_hosts.clear()

    #     all_name_hosts = list(self.cluster.hosts.items())
    #     max_workers = cpu_count()
    #     num_of_hosts = len(all_name_hosts)

    #     find_suitable_nodes_socket = partial(self.par_find_suitable_nodes, socket_conf)

    #     #print("FSN: 1")
    #     futures = list()

    #     if num_of_hosts > max_workers:
    #         chunk_size = ceil(num_of_hosts/max_workers)
    #         for i in range(max_workers):
    #             futures.append(
    #                     self.executor.submit(find_suitable_nodes_socket, all_name_hosts[i:i*chunk_size])
    #             )
    #     else:
    #         chunk_size = 1
    #         for i in range(num_of_hosts):
    #             futures.append(
    #                     self.executor.submit(find_suitable_nodes_socket, [all_name_hosts[i]])
    #             )

    #     # Wait until all of those process end
    #     wait(futures)

    #     #print("FSN: 4")

    #     return dict(self.mgr_suitable_hosts), self.mgr_req_cores.get() <= 0

    def host_alloc_condition(self, hostname: str, job: Job) -> float:
        """Condition on which hosts to use first for allocation.
        """
        return 1.0

    def allocation(self, job: Job, socket_conf: tuple, immediate=False) -> bool:
        """We allocate first to the idle hosts and then to the in use hosts
        """
        """Co-allocate a job to an executing job under a specific socket mapping/configuration
        + job: the job we want to co-allocate
        + socket_conf: socket mapping/configuration for the job
        """

        job.socket_conf = socket_conf

        # Get only the suitable hosts
        suitable_hosts, req_okay = self.find_suitable_nodes(job.num_of_processes, 
                                                            socket_conf, immediate=immediate)

        # If no suitable hosts where found
        if not req_okay:
            return False

        # Apply the colocation condition
        suitable_hosts = dict(
                sorted(suitable_hosts.items(), key=lambda it: self.host_alloc_condition(it[0], job), reverse=True)
        )

        # Calculate how many cores per node and the number 
        # of nodes needed to satisfy the job
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

    def compact_allocation(self, job: Job, immediate=False) -> bool:
        """Compact and exclusive allocation of a job
        """
        return self.allocation(job, self.cluster.full_socket_allocation, immediate=immediate)

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
        # Parallelize
        max_workers = cpu_count()
        self.mgr_suitable_hosts = list(self.cluster.hosts.items())
        num_of_hosts = len(self.mgr_suitable_hosts)
        if num_of_hosts > max_workers:
            # self.chunk_size_ref = ray.put(ceil(num_of_hosts/max_workers))
            self.max_workers = max_workers
        else:
            # self.chunk_size_ref = ray.put(1)
            self.max_workers = num_of_hosts

    def waiting_queue_reorder(self, job: Job) -> float:
        """How to re-order the jobs inside the waiting queue
        """
        return 1.0

    @abstractmethod
    def deploy(self) -> bool:
        """Abstract method to deploy the new execution list to the cluster
        """
        pass

    def backfill(self) -> bool:
        """A backfill algorithm for the scheduler
        """
        return False
