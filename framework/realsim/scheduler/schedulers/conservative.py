import os
import sys
from functools import reduce

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))

from realsim.cluster.host import Host
from realsim.jobs.utils import deepcopy_list
from .fifo import FIFOScheduler
from math import inf


class ConservativeScheduler(FIFOScheduler):

    name = "Conservative Scheduler"
    description = "FIFO Scheduler with conservative backfilling policy"

    def __init__(self):
        FIFOScheduler.__init__(self)
        self.backfill_enabled = True

    def reserve(self, free_slots, execution_list, blocked_job):

        execution_list.sort(key=lambda job: job.wall_time + job.start_time - self.cluster.makespan)
        min_estimated_time = inf
        rslots = free_slots # Reservation slots

        for xjob in execution_list:

            rslots += xjob.assigned_hosts

            if len(rslots) >= blocked_job.full_socket_nodes:
                min_estimated_time = xjob.wall_time - (self.cluster.makespan - xjob.start_time)
                break

        # If a job couldn't reserve cores then cancel backfill at this point
        if not min_estimated_time < inf:
            return False
        else:
            execution_list.append(blocked_job)
            free_slots -= rslots

    # find_reservation([], 0, blocked_job, idle_hosts, waiting_queue, execution_list)

    def find_reservation(self, reserves, start_time, blocked_job, free_slots, waiting_queue, execution_list):

        if not start_time < inf:
            return reserves

        # Calculate the estimated start time of blocked job
        execution_list.sort(key=lambda job: job.wall_time + job.start_time - self.cluster.makespan)
        min_estimated_time = inf
        rslots = free_slots
        jobs_to_finish = list()

        for xjob in execution_list:

            rslots += xjob.assigned_hosts
            jobs_to_finish.append(xjob)

            if len(rslots) >= blocked_job.full_socket_nodes:
                min_estimated_time = xjob.wall_time - (self.cluster.makespan - xjob.start_time)
                break

        reserves.append(start_time + min_estimated_time)
        free_slots = [name for name in free_slots if name not in rslots]
        
        for xjob in execution_list:
            xjob.remaining_time -= min_estimated_time
        execution_list = [xjob for xjob in execution_list if xjob.remaining_time > 0]
        execution_list.append(blocked_job)
        
        if waiting_queue != []:
            blocked_job = waiting_queue[0]
        else:
            return reserves
        waiting_queue.remove(blocked_job)

        return self.find_reservation(
                reserves,
                start_time + min_estimated_time,
                blocked_job,
                free_slots,
                waiting_queue,
                execution_list
        )


    def backfill(self) -> bool:

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            return False

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])
        blocked_job = waiting_queue[0]
        waiting_queue.remove(blocked_job)
        execution_list = deepcopy_list(self.cluster.execution_list)

        # Get all the idle hosts
        idle_hosts = [host for host in list(self.cluster.hosts.values()) if host.state == Host.IDLE]

        reserves = self.find_reservation([], 0, blocked_job, idle_hosts, waiting_queue, execution_list)

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        for i, rtime in enumerate(reserves):

            if backfilling_jobs[i].wall_time <= rtime:

                if self.compact_allocation(backfilling_jobs[i]):
                    deployed = True
        
        return deployed
