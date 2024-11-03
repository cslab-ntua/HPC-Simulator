# Global libraries
import abc
from numpy.random import seed
from time import time_ns
from typing import TypeVar, Generic
from collections.abc import Callable

from .__init__ import *
from math import inf

T = TypeVar("T")


class AbstractGenerator(abc.ABC, Generic[T]):

    name = "Abstract Generator"
    description = "Abstract base class for all generators"

    def __init__(self, 
                 load_manager: LoadManager, 
                 timer: Callable[[], float] = lambda: inf):
        self.load_manager = load_manager
        self._timer = timer

    @property
    def timer(self):
        return self._timer()

    @timer.setter
    def timer(self, timer: Callable[[], float]):
        self._timer = timer

    def generate_job(self, idx: int, load: Load) -> Job:
        seed(time_ns() % (2**32))
        job =  Job(job_id=idx,
                   job_name=load.load_name,
                   num_of_processes=load.num_of_processes,
                   assigned_hosts=list(),
                   remaining_time=load.get_med_time(),
                   submit_time=0,
                   waiting_time=0,
                   wall_time=(1.25 * load.get_med_time()))
        job.job_tag = load.get_tag()

        return job

    @abc.abstractmethod
    def generate_jobs_set(self, arg: T) -> list[Job]:
        """Generate a set of num_of_jobs jobs based on the workloads stored in load_manager
        """
        pass
