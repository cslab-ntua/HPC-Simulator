import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__name__), "..", "..")
))

from realsim.generators import *
from realsim.generators.abstract import AbstractGenerator
from random import shuffle, seed
from time import time_ns

class KeysListGenerator(AbstractGenerator[str]):

    name = "List Generator"
    description = "Generate jobs based on the list of names given by the user"

    def __init__(self, load_manager):
        AbstractGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: str) -> list[Job]:
        """Generate jobs based on the names in the dictionary and their
        frequency of appeareance.

        - arg: a list of names of loads and their submission time
        """

        # Create a list of jobs based on arg
        jobs_set = list()
        text_split = arg.split('\n')

        for line in text_split[1:]:
            fields = line.split(',')
            if len(fields) < 18:
                continue
            job = self.generate_job(int(fields[0]), self.load_manager(fields[13]))
            job.submit_time = float(fields[1])
            job.wall_time = float(fields[8])

            jobs_set.append(job)
            
        seed(time_ns() % (2 ** 32))
        shuffle(jobs_set)
        # for i, _job in enumerate(jobs_set):
        #   _job.submit_time = i

        return jobs_set

