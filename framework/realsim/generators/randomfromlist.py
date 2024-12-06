import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__name__), "..", "..")
))

from realsim.generators import *
from realsim.generators.ACustomLogs import AbstractCustomLogsGenerator
from numpy.random import seed, randint, random_sample
from time import time_ns


class RandomFromListGenerator(AbstractCustomLogsGenerator[list]):

    name = "Random From List Generator"
    description = "Generating random set of jobs from a specific LoadManager instance"

    def __init__(self, load_manager: LoadManager):
        AbstractCustomLogsGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: list) -> list[Job]:
        # Get the load names of the load_manager
        keys = list(filter(None, arg[1].split('\n')))
        # Generate random positive integers that will be used as
        # indices to query the loads' names
        seed(time_ns() % (2 ** 32))
        ints = randint(low=0, high=len(keys), size=(arg[0],))

        # Get the names of the loads
        names = list(map(lambda i: keys[i], ints))
        # Get the loads
        loads = list(map(lambda name: self.load_manager(name), names))

        jobs_set: list[Job] = list()
        for i, load in enumerate(loads):
            jobs_set.append(
                    self.generate_job(i, load)
            )

        return jobs_set
