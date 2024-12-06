# Global libraries
from abc import abstractmethod
from numpy.random import seed
from time import time_ns
from collections.abc import Callable
from typing import TypeVar, Generic

import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__name__), "..", "..")
))
from realsim.generators.AGenerator import AbstractGenerator

from .__init__ import *
from math import inf

T = TypeVar("T")


class AbstractCustomLogsGenerator(AbstractGenerator, Generic[T]):

    name = "Abstract Generator"
    description = "Abstract base class for all generators"

    def __init__(self, 
                 load_manager: LoadManager, 
                 timer: Callable[[], float] = lambda: inf):
        AbstractGenerator.__init__(self, timer)
        self.load_manager = load_manager

    def generate_job(self, idx: int, load: Load) -> Job:
        seed(time_ns() % (2**32))
        job =  Job(job_id=idx,
                   job_name=load.load_name,
                   num_of_processes=load.num_of_processes,
                   assigned_hosts=list(),
                   remaining_time=load.get_med_time(),
                   submit_time=0,
                   waiting_time=0,
                   wall_time=(1.4 * load.get_med_time()))
        job.job_tag = load.get_tag()

        return job

    @abstractmethod
    def generate_jobs_set(self, arg: T) -> list[Job]:
        """Generate a set of num_of_jobs jobs based on the workloads stored in load_manager
        """
        pass
