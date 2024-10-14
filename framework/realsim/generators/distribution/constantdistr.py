import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
))

from realsim.generators.distribution.idistribution import IDistribution


class ConstantDistribution(IDistribution):

    def apply_distribution(self, jobs_set, **kwargs):
        # Get the constant time step
        time_step = float(kwargs["time_step"])
        # Get the first submission time
        submit_time = jobs_set[0].submit_time
        for job in jobs_set:
            job.submit_time = submit_time
            submit_time += time_step
        return jobs_set
