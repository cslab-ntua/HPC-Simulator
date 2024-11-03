from numpy.random import exponential
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
))

from realsim.generators.distribution.idistribution import IDistribution


class PoissonDistribution(IDistribution):

    def apply_distribution(self, jobs_set, **kwargs):
        # Get time step
        time_step = float(kwargs["time_step"])
        interpacket_diff = len(jobs_set) * time_step
        current_time = exponential(time_step)
        i = 0
        for job in jobs_set:
            if i % len(jobs_set) == 0:
                current_time += interpacket_diff
            job.submit_time = current_time
            current_time += exponential(time_step)
            i += 1

        return jobs_set
