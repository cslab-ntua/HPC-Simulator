from numpy.random import seed, uniform
from time import time_ns
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
))

from realsim.generators.distribution.idistribution import IDistribution


class RandomDistribution(IDistribution):

    def apply_distribution(self, jobs_set, **kwargs):
        # Get time step
        time_step = float(kwargs["time_step"])

        seed(time_ns() % (2 ** 32))
        current_time = uniform(low=0, high=time_step, size=(1,))[0]
        for job in jobs_set:
            job.submit_time = current_time
            seed(time_ns() % (2 ** 32))
            current_time += uniform(low=0, high=time_step, size=(1,))[0]

        return jobs_set
