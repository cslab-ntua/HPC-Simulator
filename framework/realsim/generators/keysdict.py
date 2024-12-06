import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__name__), "..", "..")
))

from realsim.generators import *
from realsim.generators.ACustomLogs import AbstractCustomLogsGenerator
from random import shuffle, seed
from time import time_ns

class KeysDictGenerator(AbstractCustomLogsGenerator[dict]):

    name = "Dictionary Generator"
    description = "Generate jobs by setting their frequency inside the set"

    def __init__(self, load_manager):
        AbstractCustomLogsGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: dict[str, int]) -> list[Job]:
        """Generate jobs based on the names in the dictionary and their
        frequency of appeareance.

        - arg: a dictionary with names of loads and their frequency of
          appeareance
        """

        # Create a list of jobs based on arg
        idx = 0
        jobs_set: list[Job] = list()
        for load_name, freq in arg.items():
            for _ in range(freq):
                jobs_set.append(
                        self.generate_job(idx, self.load_manager(load_name))
                )
                idx += 1

        seed(time_ns() % (2 ** 32))
        shuffle(jobs_set)

        return jobs_set

