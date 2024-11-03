"""
Utility function used in various instances of the source code for Jobs inside
containers
"""

from .jobs import Job


def deepcopy_list(jobs_list: list[Job]):
    """
    Create and return a new list of jobs or lists of jobs.
    This function tries to fit all the oddities found in the simulation code
    and provide a single way of copying such lists
    ---
    Different lists of jobs are found in the simulation such as the waiting 
    queue and the execution list.
    """

    # Nothing to copy if the list is empty 
    # but return a new empty list for reference
    if jobs_list == []:
        return []

    new_list = list()
    for job in jobs_list:
        new_list.append(job.deepcopy())

    # If everything turns out okay then return the new list
    return new_list

