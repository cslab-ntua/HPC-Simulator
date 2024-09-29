import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from procset import ProcSet


class JobCharacterization:
    COMPACT = 0
    SPREAD = 1
    ROBUST = 2
    FRAIL = 3

class JobState:
    PENDING = 0
    EXECUTING = 1
    FINISHED = 2
    FAILED = 3
    ABORTED = 4


class Job:
    """Class that simulates an HPC job
    """

    def __init__(self, 
                 job_id: int, 
                 job_name: str, 
                 num_of_processes: int,
                 assigned_hosts: set[str],
                 full_node_cores: int,
                 half_node_cores: int,
                 remaining_time, 
                 submit_time, 
                 waiting_time, 
                 wall_time):

        # Important identifiers of the job
        self.job_id = job_id
        self.job_name = job_name
        self.job_signature = f"{job_id}:{job_name}"

        # Cores/Nodes resources
        self.num_of_processes = num_of_processes
        self.assigned_hosts = assigned_hosts
        self.socket_conf = tuple()

        # Time resources
        self.remaining_time = remaining_time
        self.submit_time = submit_time
        self.waiting_time = waiting_time
        self.wall_time = wall_time
        self.start_time: float = 0.0
        self.finish_time: float = -1.0

        # Speedups of job
        self.sim_speedup: float = 1
        self.avg_speedup: float = 1
        self.max_speedup: float = 1
        self.min_speedup: float = 1

        # Job performance tag
        self.job_tag = list()

        # Job characterization for schedulers
        self.job_character = JobCharacterization.COMPACT

        # If head job of waiting queue reaches a certain age then 
        # change from co-schedule policy to compact allocation policy
        self.age = 0


    def __eq__(self, job):
        if not isinstance(job, Job):
            return False
        return  self.job_id == job.job_id\
                and self.job_name == job.job_name\
                and self.num_of_processes == job.num_of_processes\
                and self.assigned_hosts == job.assigned_hosts\
                and self.remaining_time == job.remaining_time\
                and self.submit_time == job.submit_time\
                and self.wall_time == job.wall_time\
                and self.start_time == job.start_time\
                and self.sim_speedup == job.sim_speedup\
                and self.avg_speedup == job.avg_speedup\
                and self.max_speedup == job.max_speedup\
                and self.min_speedup == job.min_speedup\
                and self.job_tag == job.job_tag\
                and self.job_character == job.job_character
                # and self.assigned_cores == job.assigned_cores\

    def __repr__(self) -> str:
        return f"[{self.job_id}:{self.job_name}],(T:{self.remaining_time}),(C:{len(self.assigned_cores)}),(S:{self.sim_speedup})"

    def get_avg_speedup(self) -> float:
        return self.avg_speedup

    def get_max_speedup(self) -> float:
        return self.max_speedup

    def get_min_speedup(self):
        return self.min_speedup

    def deepcopy(self):
        """Return a new instance of Job that is a true copy
        of the original
        """
        copy = Job(job_id=self.job_id,
                   job_name=self.job_name,
                   num_of_processes=self.num_of_processes,
                   assigned_hosts=self.assigned_hosts,
                   assigned_cores=self.assigned_cores,
                   full_node_cores=self.full_node_cores,
                   half_node_cores=self.half_node_cores,
                   remaining_time=self.remaining_time,
                   submit_time=self.submit_time,
                   waiting_time=self.waiting_time,
                   wall_time=self.wall_time)

        copy.start_time = self.start_time
        copy.sim_speedup = self.sim_speedup
        copy.avg_speedup = self.avg_speedup
        copy.max_speedup = self.max_speedup
        copy.min_speedup = self.min_speedup
        copy.job_tag = self.job_tag
        copy.job_character = self.job_character
        copy.age = self.age

        return copy


class EmptyJob(Job):

    def __init__(self, job: Job):
        Job.__init__(self, 
                     job.job_id, 
                     job.job_name, 
                     job.num_of_processes, 
                     job.assigned_hosts,
                     job.assigned_cores,
                     -1, 
                     -1, 
                     None, 
                     None, 
                     None, 
                     None)

    def __repr__(self) -> str:
        return f"[{self.job_id}:{self.job_name}],(T:{self.remaining_time}),(C:{len(self.assigned_cores)}),(S:{self.sim_speedup})"

    def deepcopy(self):
        """Return a new instance of Job that is a true copy
        of the original
        """
        copy = EmptyJob(Job(job_id=self.job_id,
                            job_name=self.job_name,
                            num_of_processes=self.num_of_processes,
                            assigned_hosts=self.assigned_hosts,
                            assigned_cores=self.assigned_cores,
                            full_node_cores=self.full_node_cores,
                            half_node_cores=self.half_node_cores,
                            remaining_time=self.remaining_time,
                            submit_time=self.submit_time,
                            waiting_time=self.waiting_time,
                            wall_time=self.wall_time)
                        )

        return copy
