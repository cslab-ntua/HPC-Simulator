import os
import sys
from typing import Optional
from functools import reduce

from procset import ProcSet
sys.path.append(
        os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
)

from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.cluster.host import Node

cluster = ClusterExhaustive(10, (4,4))
for hostname, host in cluster.hosts.items():
    print(hostname, host.sockets)


def find_suitable_nodes(req_cores: int, hosts: dict[str, Node], socket_conf: tuple) -> list[tuple[str, list[ProcSet]]]:
    """ Allocate nodes and the cores inside those nodes
    """
    cores_per_host = sum(socket_conf)
    to_be_allocated = list()
    for hostname, host in hosts.items():
        if reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, host.sockets))):
            req_cores -= cores_per_host
            to_be_allocated.append((hostname, [
                ProcSet.from_str(' '.join([str(x) for x in p_set[:socket_conf[i]]]))
                for i, p_set in enumerate(host.sockets)]
            ))

    # If the amount of cores needed is covered then return the list of possible
    # hosts
    if req_cores <= 0:
        return to_be_allocated
    # Else, if not all the cores can be allocated return an empty list
    else:
        return []


try:
    for hostname, procsets in find_suitable_nodes(20, cluster.hosts, cluster.half_socket_allocation):
        for i, pset in enumerate(procsets):
            cluster.hosts[hostname].sockets[i] -= pset
except:
    print("Couln't find a list of hosts satisfying the needs of the job")

print(find_suitable_nodes(40, cluster.hosts, cluster.half_socket_allocation))
