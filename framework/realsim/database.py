"""
The database will be responsible of storing a pre-loaded queue of jobs at the
initialisation of a simulation. It will also store important information about
jobs.

v1.0 : For starters, the information about the heatmap of the jobs in the
pre-loaded queue will be stored.

v1.1 (!next!) : store info about allocation of jobs (co-scheduling) so that the
execution list will be a list of floating number and not a list of lists. Also
the jobs will be addressed by their ids.
"""

import os
import sys
from typing import Optional, Protocol

# Set the root directory of the api library
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../"
)))

from api.loader import Load

# Set the root directory of the realsim library
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

# Import library components
from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list

Heatmap = dict[str,dict[str, Optional[float]]]

# Define the inference engine
class InferenceEngine(Protocol):
    def predict(self, X):
        pass


class Database:

    def __init__(self, 
                 jobs_set: list[Job], 
                 heatmap: Heatmap = dict(),
                 engine: Optional[InferenceEngine] = None):
        self.preloaded_queue = deepcopy_list(jobs_set)
        self.heatmap = heatmap
        self.engine = engine

    def pop(self, queue: list[Job]) -> Job:
        job: Job = queue[0]
        queue.remove(job)
        return job

    def init_heatmap(self):

        # If there is an inference engine and the heatmap is not populated
        # with values
        if self.engine is not None and self.heatmap == dict():

            # Initialize the heatmap
            for job in self.preloaded_queue:
                self.heatmap[job.job_name] = {}

            # Get a copy of the preloaded queue
            preloaded_queue = deepcopy_list(self.preloaded_queue)

            while preloaded_queue != []:

                job: Job = self.pop(preloaded_queue)

                for co_job in preloaded_queue:

                    # If an inference engine is provided then predict the
                    # speedup for both load and co-load when co-scheduled

                    # Get speedup for load when co-scheduled with co-load
                    tag = list()
                    tag.extend(job.job_tag)
                    tag.extend(co_job.job_tag)
                    self.heatmap[job.job_name].update({
                            co_job.job_name: self.engine.predict(tag)
                    })

                    # Get speedup for co-load when co-scheduled with load
                    co_tag = list()
                    co_tag.extend(co_job.job_tag)
                    co_tag.extend(job.job_tag)
                    self.heatmap[co_job.job_name].update({
                            job.job_name: self.engine.predict(co_tag)
                    })

    def setup(self):
        self.init_heatmap()
