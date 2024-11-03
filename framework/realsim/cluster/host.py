from procset import ProcSet

class Host:

    # Host states
    IDLE = 0
    ALLOCATED = 1
    DOWN = 2

    def __init__(self,
                 socket_conf: tuple, 
                 first_core_id: int):
        
        self.socket_conf = socket_conf
        self.sockets: list[ProcSet] = list()

        # Define sockets
        _count = first_core_id
        for cores in socket_conf:
            self.sockets.append(ProcSet((_count, _count + cores - 1)))
            _count += cores

        # Set starting state of a host
        self.state = Host.IDLE

        # Get references of the jobs running on the host
        self.jobs: dict[str, list[ProcSet]] = dict()
 
    def get_idle_cores_num(self) -> int:
        _sum = 0
        for pset in self.sockets:
            _sum += len(pset)

        return _sum

    def get_used_cores_num(self) -> int:
        return sum(self.socket_conf) * len(self.sockets) - self.get_idle_cores_num()


# Alias for Host class
Node = Host
