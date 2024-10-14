import os
import sys
from abc import ABC, abstractmethod

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
))

from realsim.jobs.jobs import Job

class IDistribution(ABC):
    """Interface for Distribution classes
    """

    @abstractmethod
    def apply_distribution(self, jobs_set: list[Job], **kwargs) -> list[Job]:
        pass
