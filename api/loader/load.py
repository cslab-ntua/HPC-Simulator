from typing import Optional, TypeVar
from numpy import average as avg
from numpy import median
from json import dumps, loads

T = TypeVar("T", 'Load', str)


class Load:
    """Load is a class that stores all the information gained by the logs of
    past runs for a specific benchmark; be it executing exclusively or
    resource-sharing with another benchmark.
    
    The information stored is:
    1. General information about the name, the number of processes needed, the
    machine it ran on and benchmark suite
    2. The exclusive and resource sharing execution times for each pairing
    3. Performance counters deemed necessary
    4. MPI events deemed necessary
    """

    def __init__(self, 
                 load_name: str, 
                 num_of_processes: int,
                 machine: str,
                 suite: str):

        # Load general information
        self.load_name = load_name
        self.num_of_processes = num_of_processes
        self.machine = machine
        self.suite = suite

        # Load's time in compact exclusive execution
        # spread and co-scheduled with other logs
        self.compact_timelogs: list[float] = []
        self.spread_timelogs: list[float] = []
        self.coscheduled_timelogs: dict[str, list] = dict()

        # Perf events
        self.dpops: int = 0
        self.bytes_transferred: int = 0
        self.ipc: float = 0

        # MPI attributes
        # General normalized timers
        self.compute_time_norm: float = 0
        self.mpi_time_norm: float = 0

        # MPI events' names
        mpi_events = [
                "mpi_allgather",
                "mpi_allreduce",
                "mpi_alltoall",
                "mpi_barrier",
                "mpi_bcast",
                "mpi_comm_dup",
                "mpi_comm_free",
                "mpi_comm_split",
                "mpi_dims_create",
                "mpi_irecv",
                "mpi_isend",
                "mpi_recv",
                "mpi_reduce",
                "mpi_scan",
                "mpi_send",
                "mpi_wait",
                "mpi_waitall"
        ]
        # MPI events : number of calls
        self.mpi_noc: dict[str, int] = dict()
        # MPI events : aggregated time (milliseconds)
        self.mpi_atime: dict[str, float] = dict()
        # MPI events : aggregated bytes
        self.mpi_abytes: dict[str, int] = dict()

        for name in mpi_events:
            self.mpi_noc[name] = 0
            self.mpi_atime[name] = 0
            self.mpi_abytes[name] = 0

    def __str__(self) -> str:
        return self.load_name

    def __repr__(self) -> str:
        return f"""\033[1m{self.load_name}:\033[0m
⊙ Suite: {self.suite}
⊙ Machine: {self.machine}
⊙ Number of processes: {self.num_of_processes}
⊙ Avg DP FLOPs/s: {(self.get_avg_dp_FLOPS() / 10 ** 9):.4f} GFLOPS
⊙ Avg Bytes/s: {(self.get_avg_dram_bandwidth() / 2 ** 30):.4f} GB/s
⊙ Avg IPC: {self.ipc / self.get_avg_time()}
⊙ MPI Communication: {(self.mpi_time_norm * 100):.4f}%
⊙ Coloads: {list(self.coscheduled_timelogs.keys())}"""

    def __call__(self, co_load: Optional[T] = None, policy='cmp') -> list[float]:
        """Get the compact or co-scheduled timelogs for load

        ⟡ co_load: can be a Load instance, a string with the name of the load or
        NoneType. If None the compact timelogs are return. If it is a Load
        instance or a string with the name of a load then the co-scheduled 
        timelogs are returned.
        """
        if co_load is None:
            if policy=='cmp':
                return self.compact_timelogs
            else:
                return self.spread_timelogs
        else:
            # Return the co-scheduled timelogs
            return self.coscheduled_timelogs[str(co_load)]

    def __eq__(self, load: object) -> bool:
        """Return true if a Load instance is equal to ourselves

        ⟡ load: the pythonic object we are comparing to
        """
        if type(load) != Load:
            # If not of the same type return false
            return False
        else:

            # Else test the conditions
            condition = (self.load_name == load.load_name)
            condition &= (self.num_of_processes == load.num_of_processes)
            condition &= (self.suite == load.suite)
            condition &= (self.machine == load.machine)
            condition &= (self.compact_timelogs == load.compact_timelogs)
            condition &= (self.spread_timelogs == load.spread_timelogs)
            condition &= (self.coscheduled_timelogs == load.coscheduled_timelogs)
            condition &= (self.dpops == load.dpops)
            condition &= (self.bytes_transferred == load.bytes_transferred)
            condition &= (self.ipc == load.ipc)
            condition &= (self.compute_time_norm == load.compute_time_norm)
            condition &= (self.mpi_time_norm == load.mpi_time_norm)
            condition &= (self.mpi_noc== load.mpi_noc)
            condition &= (self.mpi_atime== load.mpi_atime)
            condition &= (self.mpi_abytes == load.mpi_abytes)

            return condition

    def deepcopy(self) -> 'Load':
        """Deepcopy of a Load instance
        """

        # Create a new instance of class Load
        new_load = Load(load_name=self.load_name,
                        num_of_processes=self.num_of_processes,
                        machine=self.machine,
                        suite=self.suite)

        # Deepcopy the compact timelogs
        new_load.compact_timelogs.extend(self.compact_timelogs)
        new_load.spread_timelogs.extend(self.spread_timelogs)
        # Deepcopy the co-scheduled timelogs
        for name, value in self.coscheduled_timelogs.items():
            new_value = list()
            new_value.extend(value.copy())
            new_load.coscheduled_timelogs[name] = new_value

        # Copy load's attributes to ret_load
        new_load.dpops = self.dpops
        new_load.bytes_transferred = self.bytes_transferred
        new_load.ipc = self.ipc
        new_load.compute_time_norm = self.compute_time_norm
        new_load.mpi_time_norm = self.mpi_time_norm

        # Deep copy of MPI attributes' dicts
        for event in list(self.mpi_noc.keys()):
            new_load.mpi_noc[event] = self.mpi_noc[event]
            new_load.mpi_atime[event] = self.mpi_atime[event]
            new_load.mpi_abytes[event] = self.mpi_abytes[event]

        return new_load

    def get_avg_time(self, co_load: Optional[T] = None, policy='cmp') -> float:
        """Get the average execution time when compact/spread or co-scheduled with
        another load

        ⟡ co_load: can be a Load instance, a string with the name of the load or
        NoneType. If None the compact average execution time is returned. If it 
        is a Load instance or a string with the name of a load then the 
        average co-scheduled execution time is returned.
        """
        if co_load is None:
            if policy=='cmp':
                return float( avg(self.compact_timelogs) )
            else:
                if self.spread_timelogs:
                    return float( avg(self.spread_timelogs) )
                else:
                    return None
        else:
            # Get co-scheduled average execution time
            return float(
                    avg(list(map(
                        lambda logs: avg(logs), 
                        self.coscheduled_timelogs[str(co_load)]
                    )))

            )

    def get_med_time(self, co_load: Optional[T] = None, policy='cmp') -> float:
        """Get the median execution time when compact/spread or co-scheduled with
        another load

        ⟡ co_load: can be a Load instance, a string with the name of the load or
        NoneType. If None the compact median execution time is returned. If it 
        is a Load instance or a string with the name of a load then the 
        median co-scheduled execution time is returned.
        """
        if co_load is None:
            if policy=='cmp':
                return float( median(self.compact_timelogs) )
            else:
                if self.spread_timelogs:
                    return float( median(self.spread_timelogs) )
                else:
                    return None
        else:
            if (self.load_name == str(co_load)):
                return float(
                        avg(list(map(
                            lambda logs: median(logs),
                            self.coscheduled_timelogs[str(co_load)]
                        )))
                )
            else:
                concat_list = list()
                for li in self.coscheduled_timelogs[str(co_load)]:
                    concat_list += li
                return float(median(concat_list))

    def get_avg_speedup(self, co_load: Optional[T] = None) -> float:
        """Return the average speedup when co-scheduled with a load or spread if None
                     ⟡ co_load: the co_scheduled load; Load or str
        """
        if co_load:
            return (self.get_avg_time() / self.get_avg_time(str(co_load)))
        else: # spread
            try:
                return (self.get_avg_time() / self.get_avg_time(policy='spd'))
            except:
                return None

    def get_med_speedup(self, co_load: Optional[T] = None) -> float:
        """Return the median speedup when co-scheduled with a load or spread if None
                     ⟡ co_load: the co_scheduled load; Load or str
        """
        if co_load:
            return (self.get_med_time() / self.get_med_time(str(co_load)))
        else: # spread
            try:
                return (self.get_med_time() / self.get_med_time(policy='spd'))
            except:
                return None

    def get_avg_dram_bandwidth(self, policy='cmp') -> float:
        """Get the average DRAM bandwidth
        """
        # Calculate the bandwidth for all the compact time-logs
        if policy=='cmp':
            bw_list = list(map(lambda log: 
                           self.bytes_transferred / log,
                           self.compact_timelogs))
        else:
            bw_list = list(map(lambda log: 
                           self.bytes_transferred / log,
                           self.spread_timelogs))

        return float( avg(bw_list) )

    def get_avg_dp_FLOPS(self, policy='cmp') -> float:
        """Get the average double precision FLOPS
        """
        # Calculate the DP-FLOPS for all the compact time-logs
        if policy=='cmp':
            dpops_list = list(map(lambda log: 
                           self.dpops / log,
                           self.compact_timelogs))
        else:
            dpops_list = list(map(lambda log: 
                           self.dpops / log,
                           self.spread_timelogs))

        return float( avg(dpops_list) )

    def get_tag(self, policy='cmp') -> list:
        return [self.get_med_time(policy=policy), 
                self.mpi_time_norm, 
                self.ipc, 
                self.get_avg_dp_FLOPS(), 
                self.get_avg_dram_bandwidth()]

    def set_coload(self, co_load: T, time_bundle=[]) -> None:
        """Store the name of a co-scheduled load and the execution time for each
        run of the load
        """
        self.coscheduled_timelogs[str(co_load)] = time_bundle

    def to_json(self) -> str:
        repres =\
        {
                "load_name": self.load_name,
                "num_of_processes": self.num_of_processes,
                "machine": self.machine,
                "suite": self.suite,
                "compact_timelogs": self.compact_timelogs,
                "spread_timelogs": self.spread_timelogs,
                "coscheduled_timelogs": self.coscheduled_timelogs,
                "dpops": self.dpops,
                "bytes_transferred": self.bytes_transferred,
                "ipc": self.ipc,
                "compute_time_norm": self.compute_time_norm,
                "mpi_time_norm": self.mpi_time_norm
        }
        return dumps(repres)

    def inject_json(self, json_repres: str) -> None:
        repres = loads(json_repres)
        self.load_name = repres["load_name"]
        self.num_of_processes = repres["num_of_processes"]
        self.machine = repres["machine"]
        self.suite = repres["suite"]
        self.compact_timelogs = repres["compact_timelogs"]
        self.spread_timelogs = repres["spread_timelogs"]
        self.coscheduled_timelogs = repres["coscheduled_timelogs"]
        self.dpops = repres["dpops"]
        self.bytes_transferred = repres["bytes_transferred"]
        self.ipc = repres["ipc"]
        self.compute_time_norm = repres["compute_time_norm"]
        self.mpi_time_norm = repres["mpi_time_norm"]

    @classmethod
    def from_json(cls, json_repres: str) -> 'Load':
        load = Load('', 0, '', '')
        repres = loads(json_repres)
        load.load_name = repres["load_name"]
        load.num_of_processes = repres["num_of_processes"]
        load.machine = repres["machine"]
        load.suite = repres["suite"]
        load.compact_timelogs = repres["compact_timelogs"]
        load.spread_timelogs = repres["spread_timelogs"]
        load.coscheduled_timelogs = repres["coscheduled_timelogs"]
        load.dpops = repres["dpops"]
        load.bytes_transferred = repres["bytes_transferred"]
        load.ipc = repres["ipc"]
        load.compute_time_norm = repres["compute_time_norm"]
        load.mpi_time_norm = repres["mpi_time_norm"]

        return load
