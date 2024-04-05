import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.logger.logger import Logger


def run_sim(core):

    cluster, scheduler, logger = core

    cluster.setup()
    scheduler.setup()
    logger.setup()

    # The stopping condition is for the waiting queue and the execution list
    # to become empty
    while cluster.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        cluster.step()

    fig = logger.get_resource_usage(save=False)
    fig.show()

    return cluster, scheduler, logger

class Simulation:
    """The entry point of a simulation for scheduling and scheduling algorithms.
    A user can decide whether the simulation will be 'static' be creating a bag
    of jobs at the beginning or 'dynamic' by continiously adding more jobs to
    the the waiting queue of a cluster.
    """

    def __init__(self, 
                 # generator bundle
                 jobs_set,
                 # cluster
                 nodes: int, ppn: int,
                 # scheduler algorithms bundled with inputs
                 schedulers_bundle):

        self.num_of_jobs = len(jobs_set)
        self.executor = ProcessPoolExecutor(max_workers=len(schedulers_bundle))
        self.sims = dict()
        self.futures = dict()
        self.results = dict()

        for sched_class, hyperparams in schedulers_bundle:

            # Setup cluster
            cluster = ClusterExhaustive(nodes, ppn)
            cluster.preload_jobs(jobs_set)

            # Setup scheduler
            scheduler = sched_class(**hyperparams)

            # Setup logger
            logger = Logger()

            # Setup experiment
            cluster.assign_scheduler(scheduler)
            scheduler.assign_cluster(cluster)
            cluster.assign_logger(logger)
            scheduler.assign_logger(logger)

            # Record of a simulation
            self.sims[scheduler.name] = (cluster, scheduler, logger)

    def set_default(self, name):
        self.default = name

    def run(self):
        for policy, sim in self.sims.items():
            print(policy, "submitted")
            self.futures[policy] = self.executor.submit(run_sim, sim)

    def plot(self):

        figures = dict()

        # Wait until all the futures are complete
        self.executor.shutdown(wait=True)

        for policy, future in self.futures.items():

            # Get the results
            self.results[policy] = future.result()

            # Plot resource usage
            # logger = self.results[policy][2]
            # print(logger.get_history_trace())
            # fig = logger.get_resource_usage(save=False)
            # figures[f"Plot Resources: {policy}"] = fig

            # fig.show()

        # speedups = list() # makespan speedups
        # boxpoints = list()
        # compact_logger = self.results[self.default][2]

        # policies = list( self.sims.keys() )
        # policies.remove(self.default)

        # for policy in sorted(policies):
        #     logger = self.results[policy][2]
        #     speedups.append(
        #             self.results[self.default][0].makespan / self.results[policy][0].makespan
        #     )
        #     boxpoints.append( logger.get_jobs_utilization(compact_logger) )

        # fig = go.Figure()

        # for i, points in enumerate(boxpoints):
        #     names = list()
        #     s_values = list()
        #     t_values = list()
        #     for name, value in points.items():
        #         names.append(name)
        #         s_values.append(value["speedup"])
        #         t_values.append(value["turnaround"])

        #     fig.add_trace(
        #             go.Box(
        #                 y=s_values,
        #                 x=[i]*len(points),
        #                 name="speedup",
        #                 boxpoints="all",
        #                 boxmean="sd",
        #                 text=names,
        #                 marker_color="red",
        #                 showlegend=False
        #             )
        #     )

        # fig.add_trace(
        #         go.Scatter(x=list(range(len(speedups))), 
        #                    y=speedups, mode="lines+markers+text",
        #                    marker=dict(color="black"), name="Makespan Speedup"
        #         )
        # )

        # for x in range(len(speedups)):
        #     fig.add_annotation(text=f"<b>{round(speedups[x], 3)}</b>",
        #                        x=x,
        #                        y=speedups[x],
        #                        arrowcolor="black")


        # fig.add_hline(y=1, line_color="black", line_dash="dot")

        # fig.update_layout(
        #         title=f"<b>Makespan and per job speedups for {self.num_of_jobs} jobs</b>",
        #         title_x=0.5,
        #         # height=1080,
        #         # width=1920,
        #         xaxis=dict(
        #             title="<b>Co-Schedulers</b>",
        #             tickmode="array",
        #             tickvals=[x for x in range(len(policies))],
        #             ticktext=sorted(policies)
        #         ),
        #         yaxis=dict(title="<b>Speedup</b>"),
        #         template="seaborn"
        # )

        # figures["Speedups"] = fig

        # return figures


