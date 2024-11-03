import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job, JobCharacterization
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coscheduler import Coscheduler
import math

from abc import ABC


class RulesCoscheduler(Coscheduler, ABC):

    name = "Rules Co-Scheduler"
    description = """Rules-based co-scheduling """

    def __init__(self):
        Coscheduler.__init__(self)
        self.backfill_enabled = True

    def setup(self) -> None:
        return super().setup()

    def satisfies_coscheduling_rules(self, jobA: Job, jobB: Job) -> bool:
        if jobA.job_character == JobCharacterization.SPREAD and jobB.job_character == JobCharacterization.ROBUST:
            return True
        if jobA.job_character == JobCharacterization.ROBUST and jobB.job_character == JobCharacterization.SPREAD:
            return True
        if jobA.job_character == JobCharacterization.FRAIL and jobB.job_character == JobCharacterization.ROBUST:
            return True
        if jobA.job_character == JobCharacterization.ROBUST and jobB.job_character == JobCharacterization.FRAIL:
            return True

        return False

    def waiting_job_candidates_reorder(self, job: Job, co_job: Job) -> float:
        return 1.0

    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:
        return 1.0

    def after_deployment(self, *args):
        """After deploying a job in a pair or compact then some after processing
        events may need to take place. For example to calculate values necesary
        for the heuristics functions (e.g. the fragmentation of the cluster)
        """
        pass

    def best_xunit_candidate(self, job: Job) -> Optional[list[Job]]:
        """Return an executing unit (block of jobs) that is the best candidate
        for co-execution for a job. If no suitable xunit is found return None.

        + job: the job to find the best xunit candidate on its requirements
        """

        # Get xunit candidates that satisfy the resources and speedup 
        # requirements
        for xunit in self.cluster.nonfilled_xunits():

            # Get the head job (largest job) and test its number of binded cores
            # (processors) with the idle job (job doing nothing) to see if the
            # jobs inside the xunit execute as spread or co-scheduled
            #
            # Spread means that the jobs are executing at top speedup while
            # co-scheduled means that their speedup is regulated by each other's
            # resource consumption
            #
            head_job: Job = xunit[0]
            idle_job: Job = xunit[-1]

            # Number of idle processors
            # idle_cores = idle_job.binded_cores
            idle_cores = len(idle_job.assigned_cores)

            # If idle cores are less than the resources the job wants to consume
            # then the xunit is not a candidate
            if job.half_node_cores > idle_cores:
                continue

            if self.satisfies_coscheduling_rules(head_job, job):
                return xunit

        # If no candidates are found in the xunits return None
        return None

    def best_wjob_candidates(self, job: Job, waiting_queue_slice: list[Job]) -> Optional[Job]:

        for wjob in waiting_queue_slice:

            # The speedup values must exist
            conditions  = self.database.heatmap[job.job_name][wjob.job_name] is not None
            if not conditions:
                continue
            # The pair must fit in the remaining free processors of the cluster
            # conditions &= 2 * max(job.half_node_cores, wjob.half_node_cores) <= len(self.cluster.total_procs)
            conditions &= self.assign_nodes(
                    2 * max(job.half_node_cores, wjob.half_node_cores), 
                    self.cluster.total_procs) is not None
            # The pair's average speedup must be higher than the user defined
            # speedup threshold
            conditions &= self.satisfies_coscheduling_rules(wjob, job)

            # If it satisfies all the conditions then it is a candidate pairing job
            if conditions:
                return wjob

        # If no waiting job satisfies the conditions then return None
        return None

    def allocation_as_compact(self, job: Job) -> bool:

        procset = self.assign_nodes(job.full_node_cores, self.cluster.total_procs)

        # Check if the job can be allocated for compact execution
        if procset is not None:
            self.cluster.waiting_queue.remove(job)
            job.start_time = self.cluster.makespan
            job.binded_cores = job.full_node_cores
            job.assigned_cores = procset
            self.cluster.execution_list.append([job])
            self.cluster.total_procs -= procset

            return True

        else:
            return False

    def deploy(self) -> bool:

        deployed = False

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)

        while waiting_queue != []:

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            if job.job_character in [JobCharacterization.COMPACT, JobCharacterization.FRAIL]:
                # Check if it is eligible for compact allocation
                res = self.allocation_as_compact(job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue
            else:
                # Try to fit the job in an xunit
                res = self.colocation_to_xunit(job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

                # Check if there is a waiting job that can pair up with the job
                # and that they are allowed to allocate in the cluster
                res = self.colocation_with_wjobs(job, waiting_queue)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

                # Check if it is eligible for compact allocation
                res = self.allocation_as_compact(job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

            # All the allocation tries have failed. Return the job at the first
            # out position and reassign the waiting queue of the cluster
            waiting_queue.insert(0, job)
            self.cluster.waiting_queue = waiting_queue
            break

        return deployed

    def xunit_estimated_finish_time(self, xunit: list[Job]) -> float:
        """Estimated finish time for an xunit; meaning the maximum time it takes
        for all the jobs inside an xunit to finish

        + xunit: the execution unit being tested
        """

        estimations: list[float] = list()

        for job in xunit:
            if type(job) != EmptyJob:
                # Estimation is based on the worst speedup of a job
                estimate = job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan
                if estimate < 0:
                    print(estimate, job.start_time, job.wall_time / job.get_min_speedup(), self.cluster.makespan, job)
                estimations.append(estimate)

        return max(estimations)

    def backfill(self) -> bool:

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            # If there are not alternatives bail out
            return False

        blocked_job = self.cluster.waiting_queue[0]

        execution_list = deepcopy_list(self.cluster.execution_list)
        # The blocked job can be co-scheduled
        # This means it can either fit inside an existing execution unit
        # or it waits until whole xunits finish execution

        # Xunits that the blocked job can fit in
        xunits_for_colocation = list()
        estimated_start_time_coloc = math.inf

        # Xunits that the job cannot fit in and they will be merged in order
        # for the job to have enough free space to execute properly
        xunits_for_merge = list()
        estimated_start_time_merge = math.inf

        for xunit in execution_list:
            # If it compact allocated then to the mergers
            if len(xunit) == 1:
                xunits_for_merge.append(xunit)
            else:
                head_job = xunit[0]
                last_job = xunit[-1] # possible idle job
                if blocked_job.half_node_cores <= max(len(head_job.assigned_cores), len(last_job.assigned_cores)):
                    xunits_for_colocation.append(xunit)
                else:
                    xunits_for_merge.append(xunit)

        # Starting with xunits to merge we sort them by estimated finish time
        xunits_for_merge.sort(key=lambda xunit: self.xunit_estimated_finish_time(xunit))
        aggr_cores = len(self.cluster.total_procs)

        for xunit in xunits_for_merge:
            if len(xunit) == 1:
                if len(xunit[0].assigned_cores) + aggr_cores >= 2 * blocked_job.half_node_cores:
                    estimated_start_time_merge = self.xunit_estimated_finish_time(xunit)
                    break
                else:
                    aggr_cores += len(xunit[0].assigned_cores)
            else:
                xunit_binded_cores = sum([
                    len(job.assigned_cores) for job in xunit
                ])

                if xunit_binded_cores + aggr_cores >= 2 * blocked_job.half_node_cores:
                    estimated_start_time_merge = self.xunit_estimated_finish_time(xunit)
                    break
                else:
                    aggr_cores += xunit_binded_cores

        # Estimate time with xunits for colocation
        estimations = list()
        for xunit in xunits_for_colocation:
            aggr_cores = 0
            xunit_copy = deepcopy_list(xunit)
            last_job = xunit_copy[-1]
            if type(last_job) == EmptyJob:
                xunit_copy.remove(last_job)
                aggr_cores = len(last_job.assigned_cores)

            xunit_copy.sort(key=lambda job: job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan)
            for job in xunit_copy:
                if len(job.assigned_cores) + aggr_cores >= blocked_job.half_node_cores:
                    estimations.append(job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan)
                    break
                else:
                    aggr_cores += len(job.assigned_cores)

        # The estimated start time is the minimum of the two options
        # to be coscheduled in an xunit or to create an xunit
        # if estimations != []:
        #     estimated_start_time_coloc = min(estimations)
        #     estimated_start_time = min(estimated_start_time_coloc, estimated_start_time_merge)
        # else:
        #     estimated_start_time = estimated_start_time_merge

        # In finding the possible backfillers
        waiting_queue = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        while waiting_queue != []:

            backfill_job = self.pop(waiting_queue)

            if estimated_start_time_coloc is not None and\
                    estimated_start_time_coloc < estimated_start_time_merge:


                if backfill_job.job_character in [JobCharacterization.COMPACT, JobCharacterization.FRAIL]:
                    # Check if it is eligible for compact allocation
                    res = self.allocation_as_compact(backfill_job)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue
                else:
                    # Try to fit the job in an xunit
                    res = self.colocation_to_xunit(backfill_job)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

                    # Check if there is a waiting job that can pair up with the job
                    # and that they are allowed to allocate in the cluster
                    res = self.colocation_with_wjobs(backfill_job, waiting_queue)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

            else:
                if backfill_job.wall_time <= estimated_start_time_merge:
                    if backfill_job.job_character in [JobCharacterization.COMPACT, JobCharacterization.FRAIL]:
                        # Check if it is eligible for compact allocation
                        res = self.allocation_as_compact(backfill_job)

                        if res:
                            self.after_deployment()
                            deployed = True
                            continue
                    else:
                        # Try to fit the job in an xunit
                        res = self.colocation_to_xunit(backfill_job)

                        if res:
                            self.after_deployment()
                            deployed = True
                            continue

                        # Check if there is a waiting job that can pair up with the job
                        # and that they are allowed to allocate in the cluster
                        res = self.colocation_with_wjobs(backfill_job, waiting_queue)

                        if res:
                            self.after_deployment()
                            deployed = True
                            continue

        return deployed
