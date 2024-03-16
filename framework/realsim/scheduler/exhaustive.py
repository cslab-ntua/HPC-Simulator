import abc

import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

import math
from realsim.scheduler.scheduler import Scheduler
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from numpy import average as avg


class CoschedulerExhaustive(Scheduler, abc.ABC):
    """We try to provide at every checkpoint an execution list whose average
    speedup is higher than 1. We try to distribute the higher speedup candidates
    among the checkpoints.
    """

    name = "Exhaustive Co-Scheduler"
    description = "Exhaustive coupling co-scheduling base class for ExhaustiveCluster"

    def __init__(self):
        Scheduler.__init__(self)

    def assign_cluster(self, cluster: ClusterExhaustive):
        """This method is called from a cluster instance
        when it is created. It can also be used to reassign
        the scheduler to other clusters. It is essential for
        rapid experimenting.
        """
        self.cluster = cluster

    def filled_exec_units(self) -> list[list[Job]]:
        """Return all the execution units that have no empty space. All the
        binded cores are completely filled.
        """

        filled_units: list[list[Job]] = list()

        for unit in self.cluster.execution_list:

            # We don't care about compact units
            if len(unit) == 1:
                continue

            filled = True

            for job in unit:
                if type(job) == EmptyJob:
                    filled = False
                    break

            if filled:
                filled_units.append(unit)

        return filled_units

    def nonfilled_exec_units(self) -> list[list[Job]]:
        """Return all the execution units that have empty space. All the
        binded cores are not filled.
        """

        nonfilled_units: list[list[Job]] = list()

        for execution_unit in self.cluster.execution_list:

            # We don't care about compact jobs
            if len(execution_unit) == 1:
                continue

            # At least one has to be non empty and at least one empty
            non_empty = 0
            empty = 0
            for job in execution_unit:
                if type(job) == EmptyJob:
                    empty += 1
                    break
                else:
                    non_empty += 1

            if non_empty > 0 and empty > 0:
                nonfilled_units.append(execution_unit)

        return nonfilled_units

    def compact_exec_units(self) -> list[list[Job]]:
        """Return all the executing units that run under the compact allocation
        policy.
        """

        compact_list = list()

        for item in self.cluster.execution_list:
            if len(item) == 1 and type(item) != EmptyJob:
                compact_list.append(item)

        return compact_list

    @abc.abstractmethod
    def exec_nonfilled_ordering(self, *args, **kwargs) -> float:
        pass

    @abc.abstractmethod
    def exec_colocation_ordering(self, *args, **kwargs) -> float:
        pass

    @abc.abstractmethod
    def wait_queue_ordering(self, *args, **kwargs) -> float:
        pass

    @abc.abstractmethod
    def wait_colocation_ordering(self, *args, **kwargs) -> float:
        pass

    def deploying_exec_pairs(self, deploy_list,  ll_num, ll_avg_speedup):

        ########################################
        # Colocation with executing half pairs #
        ########################################

        nonfilled_units = self.nonfilled_exec_units()

        nonfilled_units.sort(
                key=lambda unit: self.exec_nonfilled_ordering(unit),
                reverse=True
        )

        for unit in nonfilled_units:

            # Get the largest job's binded cores
            max_binded_cores = unit[0].binded_cores

            # Get the sum of binded cores of the smallest jobs
            min_binded_cores = 0
            for job in unit[1:]:
                if type(job) != EmptyJob:
                    min_binded_cores += job.binded_cores

            # Find jobs in waiting queue that can fit inside the unit
            fit_jobs = list(
                    filter(
                        lambda job:
                        job.load.full_load_name in unit[0].load.coloads and
                        self.cluster.half_node_cores(job)
                        <= (max_binded_cores - min_binded_cores),
                        self.cluster.waiting_queue
                    )
            )

            fit_jobs = list(filter(
                lambda job: 
                avg([job.get_speedup(unit[0]), unit[0].get_speedup(job)]) > 1,
                fit_jobs
            ))

            # If no candidate was found then check if there is only one non
            # empty job inside the unit
            if fit_jobs == []:

                if min_binded_cores == 0: 
                    # Change to spread execution
                    if unit[0].speedup != unit[0].get_max_speedup():
                        unit[0].remaining_time *= unit[0].speedup / unit[0].get_max_speedup()
                        unit[0].speedup = unit[0].get_max_speedup()
                else:
                    # If there are other jobs except the largest that are still
                    # executing then find the job that minimizes the largest's
                    # job speedup
                    lower_jobs = list(filter(
                        lambda job: type(job) != EmptyJob,
                        unit[1:]
                    ))

                    worst_job = min(lower_jobs, key=(lambda job:
                                                     unit[0].get_speedup(job)))

                    # If the worst job hasn't finished executing
                    if unit[0].speedup != unit[0].get_speedup(worst_job):
                        unit[0].ratioed_remaining_time(worst_job)

                continue

            # Sort candidate jobs by an ordering function
            fit_jobs.sort(
                    key=(
                        lambda cojob: self.exec_colocation_ordering(
                            unit[0], cojob, ll_avg_speedup)
                        ),
                    reverse=True
            )

            # Create a substitute execution unit
            substitute_unit: list[Job] = list()

            # Load the non empty jobs
            for job in unit:
                if type(job) != EmptyJob:
                    substitute_unit.append(job)

            # Run through fit_jobs list to fit lesser jobs 
            # in the substitute_unit
            rem_cores = max_binded_cores - min_binded_cores

            for cojob in fit_jobs:
                if self.cluster.half_node_cores(cojob) <= rem_cores:
                    self.cluster.waiting_queue.remove(cojob)
                    cojob.binded_cores = self.cluster.half_node_cores(cojob)
                    cojob.ratioed_remaining_time(substitute_unit[0])

                    substitute_unit.append(cojob)
                    rem_cores -= cojob.binded_cores

            # Redefine largest job speedup
            worst_job = min(substitute_unit[1:], key=(
                lambda job2: substitute_unit[0].get_speedup(job2)
            ))
            if substitute_unit[0].speedup != substitute_unit[0].get_speedup(worst_job):
                substitute_unit[0].ratioed_remaining_time(worst_job)
            
            # If rem_cores > 0 then there is an empty space left in the unit
            if rem_cores > 0:
                empty_job = EmptyJob(Job(None, -1, "empty", rem_cores, 
                                         None, None, None, rem_cores))
                substitute_unit.append(empty_job)

            # Remove the former unit from the execution list
            self.cluster.execution_list.remove(unit)
            
            deploy_list.append(substitute_unit)

            # Write down event to logger
            self.logger.cluster_events["deploying:exec-colocation"] += 1


        return ll_num, ll_avg_speedup

    def deploying_wait_pairs(self, deploy_list, ll_num, ll_avg_speedup):

        ################################
        # Colocation with waiting jobs #
        ################################
        waiting_queue: list[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Order waiting queue by needed cores starting with the lowest
        waiting_queue.sort(key=lambda job: self.wait_queue_ordering(job),
                           reverse=True)

        # Loop until waiting queue is empty
        while waiting_queue != []:

            # Get the job at the head
            job = self.pop(waiting_queue)
            
            # Create a dummy waiting queue without `job`
            wq = deepcopy_list(self.cluster.waiting_queue)
            wq.remove(job)

            # Filter out cojobs that can't fit into the execution list as pairs
            wq = list(filter(
                lambda cojob: 
                cojob.load.full_load_name in job.load.coloads and\
                2 * max(
                    self.cluster.half_node_cores(job),
                    self.cluster.half_node_cores(cojob)
                ) <= self.cluster.free_cores, wq
            ))

            wq = list(filter(
                lambda cojob:
                #self.pmatrix_get_speedup(job.job_id, cojob.job_id) >= 1, wq
                job.get_speedup(cojob) > 0.9 and cojob.get_speedup(job) > 0.9,
                wq
            ))

            # If empty, no pair can be made continue to the next job
            if wq == []:
                continue

            # Sort `wq` by the wait_ordering function
            wq.sort(key=(lambda x: self.wait_colocation_ordering(job, x, ll_avg_speedup)),
                    reverse=True)

            execution_unit: list[Job] = [job]
            max_binded_cores = self.cluster.half_node_cores(job)
            
            # Build execution unit
            for cojob in wq:
                if self.cluster.half_node_cores(cojob) <= max_binded_cores:
                    waiting_queue.remove(cojob)
                    self.cluster.waiting_queue.remove(cojob)

                    cojob.ratioed_remaining_time(job)
                    cojob.binded_cores = self.cluster.half_node_cores(cojob)

                    execution_unit.append(cojob)

                    max_binded_cores -= cojob.binded_cores

            # If the other cojobs where larger
            # TODO: WARNING TEST OUT THE RATIOED REMAINING TIME
            if len(execution_unit) == 1:

                waiting_queue.append(job)
                
                continue
            
            self.cluster.waiting_queue.remove(job)
            
            # Set the speedup of the largest job
            worst_job = min(execution_unit[1:], key=(
                lambda cojob: execution_unit[0].get_speedup(cojob)
            ))

            execution_unit[0].ratioed_remaining_time(worst_job)
            execution_unit[0].binded_cores = self.cluster.half_node_cores(
                    execution_unit[0]
            )

            # If max_binded_cores is not 0 then fill the empty cores
            # with an empty job
            if max_binded_cores > 0:
                empty_job = EmptyJob(Job(None, -1, "empty", max_binded_cores, 
                                         None, None, None, max_binded_cores))
                execution_unit.append(empty_job)

            # Deploy them
            deploy_list.append(execution_unit)

            # Scheduler setup
            self.cluster.free_cores -= 2 * execution_unit[0].binded_cores

            # Logger cluster events update
            self.logger.cluster_events["deploying:wait-colocation"] += 1

        return ll_num, ll_avg_speedup

    def deploying_wait_compact(self, deploy_list, ll_num, ll_avg_speedup):

        #############################
        # Compact with waiting jobs #
        #############################
        waiting_queue = deepcopy_list(self.cluster.waiting_queue)
        
        # Order by the least amount of needed cores
        # waiting_queue.sort(key=(lambda job: job.num_of_processes))

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            if self.cluster.full_node_cores(job) <= int(self.cluster.free_cores):

                # Remove from cluster's waiting queue
                self.cluster.waiting_queue.remove(job)

                # Setup job
                job.binded_cores = self.cluster.full_node_cores(job)

                # Deploy job
                deploy_list.append([job])

                # Scheduler setup
                # total_deploying_cores += job.binded_cores
                self.cluster.free_cores -= job.binded_cores
                ll_avg_speedup = (ll_avg_speedup * ll_num + 1) / (ll_num + 1)
                ll_num += 1

                # Logger cluster events update
                self.logger.cluster_events["deploying:compact"] += 1

        return ll_num, ll_avg_speedup

    def deploying(self):
        """The specifications for the Balancer are as following:

        - If any job has 0 ranking then deploy it as compact.
        - Co-locate with executing half pairs: we try to find matches of the
          highest in terms of of needed cores half pairs with passable speedups.
        - Co-locate with waiting jobs: try to fit first the low in demand of
          needed cores jobs.
        - If not any of the above is true we deploy jobs in compact allocation
          policy. We start with jobs which have the lowest number of needed
          cores.
        - We will try to balance out the speedup between checkpoints by
          measuring how good a job in pairs is. It means how many pairs a job
          can construct that have an average speedup higher than 1. We want the
          jobs with highest ranking counter to be with us as much as possible.
        """

        # Important setups #

        # List of jobs to deploy
        deploy_list: List[List[Job]] = list()

        # Left list side average speedup
        ll_num, ll_avg_speedup = self.exec_avg_speedup()

        # Update ranks
        self.update_ranks()

        # Co-location with execution half pairs
        # ll_num, ll_avg_speedup = self.deploying_exec_pairs(deploy_list, 
        #                                                    ll_num,
        #                                                    ll_avg_speedup)
        
        ll_num, ll_avg_speedup = self.deploying_wait_pairs(deploy_list,
                                                           ll_num, 
                                                           ll_avg_speedup)

        deploy_len = len(deploy_list)
        
        # Deploy any job that has 0 ranking in compact allocation policy
        for job_id in list(filter(lambda jid: 
                                  self.ranks[jid] == 0,
                                  self.ranks)):
            for job in self.cluster.waiting_queue:
                if job.job_id == job_id\
                        and self.cluster.full_node_cores(job) <= self.cluster.free_cores:
                            job.binded_cores = self.cluster.full_node_cores(job)
                            deploy_list.append([job])
                            self.cluster.free_cores -= job.binded_cores
                            # Remove from the waiting queue
                            self.cluster.waiting_queue.remove(job)
        
        # If some compact jobs where found then:
        # 1. Update the ranks
        # 2. Compute the new ll_avg_speedup
        if len(deploy_list) > deploy_len:
            self.update_ranks()
            ll_avg_speedup = (ll_avg_speedup * ll_num + (len(deploy_list) - deploy_len)) / (ll_num + len(deploy_list))
            ll_num += len(deploy_list)


        #print("COLEXEC:", deploy_list)
        #print()

        # Co-location between waiting queue jobs
        # ll_num, ll_avg_speedup = self.deploying_wait_pairs(deploy_list,
        #                                                    ll_num, 
        #                                                    ll_avg_speedup)
        ll_num, ll_avg_speedup = self.deploying_exec_pairs(deploy_list,
                                                           ll_num, 
                                                           ll_avg_speedup)

        # compacts_cores = sum([
        #     unit[0].binded_cores for unit in self.cluster.execution_list
        #     if len(unit) == 1
        # ])

        # noncompacts_cores = sum([
        #     sum([job.binded_cores for job in unit]) 
        #     for unit in self.cluster.execution_list
        #     if len(unit) > 1
        # ])

        # if compacts_cores <= 0.3 * noncompacts_cores:
        # if deploy_list == [] and self.cluster.free_cores <= int(0.3 * self.cluster.total_cores):
        #     # Compact allocation of waiting queue jobs
        #     ll_num, ll_avg_speedup = self.deploying_wait_compact(deploy_list,
        #                                                          ll_num,
        #                                                      ll_avg_speedup)

        # If there are jobs to be deployed to the execution list
        # then return True
        if deploy_list != []:
            self.cluster.execution_list.extend(deploy_list)
            # Logger cluster events update
            self.logger.cluster_events["deploying:success"] += 1
            return True

        # If no job is to be deployed then False
        # Logger cluster events update
        self.logger.cluster_events["deploying:failed"] += 1
        return False

