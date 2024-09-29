import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from realsim.cluster.host import Host
from realsim.jobs.jobs import Job
from math import inf


class Cluster:

    def __init__(self, nodes: int, socket_conf: tuple):
        """
        + nodes: the number of nodes
        + socket_conf: the socket configuration; for example (10, 16) means 2
        sockets of which the first has 10 cores and the second has 16 cores
        """

        # Number of nodes
        self.nodes = nodes

        # Socket configuration
        self.socket_conf = socket_conf

        # Fast socket allocation schemes
        self.full_socket_allocation = socket_conf
        self.half_socket_allocation = tuple([int(x/2) for x in socket_conf])

        # Hosts where the hostname is a the string 'host' followed by a number
        _cores_per_node = sum(socket_conf)
        self.hosts: dict[str, Host] = {
                f"host{i}": Host(socket_conf, i * _cores_per_node + 1)
                for i in range(nodes)
        }

        # Number of current free cores
        self.free_cores = self.nodes * _cores_per_node


        # Waiting queue size
        self.queue_size = inf
        # The queue of waiting jobs
        self.waiting_queue: list[Job] = list()
        # The list of executing jobs
        self.execution_list: list[Job] = list()

        # Important counters #

        # Job id counter
        self.id_counter: int = 0
        
        # The total execution time
        # of a cluster
        self.makespan: float = 0

    def setup(self):
        self.execution_list = list()
