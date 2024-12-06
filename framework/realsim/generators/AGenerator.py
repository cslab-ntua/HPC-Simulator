# Global libraries
from abc import ABC, abstractmethod
from numpy.random import seed
from time import time_ns
from typing import TypeVar, Generic
from collections.abc import Callable

from .__init__ import *
from math import inf

T = TypeVar("T")


class AbstractGenerator(ABC, Generic[T]):

    name = "Abstract Generator"
    description = "Abstract base class for all generators"

    def __init__(self, 
                 timer: Callable[[], float] = lambda: inf):
        self._timer = timer

    @property
    def timer(self):
        return self._timer()

    @timer.setter
    def timer(self, timer: Callable[[], float]):
        self._timer = timer

    @abstractmethod
    def generate_job(self, *args, **kwargs) -> Job:
        pass

    @abstractmethod
    def generate_jobs_set(self, arg: T) -> list[Job]:
        """Generate a set of num_of_jobs jobs based on the workloads stored in load_manager
        """
        pass
