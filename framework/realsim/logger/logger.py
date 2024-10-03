import os
import sys
from functools import reduce
from typing import TYPE_CHECKING
from datetime import timedelta

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

#if TYPE_CHECKING:
#    from realsim.cluster import ClusterV2
from realsim.jobs.jobs import Job
from realsim.database import Database
from realsim.cluster.cluster import Cluster
import realsim.logger.logevts as evts
import plotly.graph_objects as go
import plotly.express.colors as colors
from procset import ProcSet

if TYPE_CHECKING:
    from realsim.scheduler.scheduler import Scheduler


class Logger(object):
    """
    Logs important events of the simulation into the memory
    for later use
    """

    def __init__(self, debug=True):
        # Controls if 
        self.debug = debug

        self.database: Database
        self.cluster: Cluster
        self.scheduler: Scheduler

        self.scale = colors.sequential.Turbo

        self.compeng_logs: list[str] = list()
        self.job_logs: list[str] = list()
        self.db_logs: list[str] = list()
        self.cluster_logs: list[str] = list()
        self.scheduler_logs: list[str] = list()

    def log(self, evt: type[evts.LogEvent], **kwargs) -> None:

        if self.debug:
            try:
                self.__dict__[evt.hook].append(evt.log(kwargs["msg"], self.cluster.makespan))
            except:
                raise RuntimeError(f"The log event specified ({evt}) doesn't exist")

        if evt == evts.JobStart:
            job: Job = kwargs["job"]
            psets: list[ProcSet] = kwargs["psets"]
            pset = reduce(lambda pA, pB: pA.union(pB), psets)
            hostname: str = kwargs["hostname"]
            self.job_events[job.job_signature]["submit time"] = job.submit_time
            self.job_events[job.job_signature]["start time"] = job.start_time
            self.job_events[job.job_signature]["waiting time"] = job.waiting_time
            self.job_events[job.job_signature]["assigned procs"] = self.job_events[job.job_signature]["assigned procs"].union(pset)
            self.job_events[job.job_signature]["hosts"].add(hostname)

        if evt == evts.JobFinish:
            job: Job = kwargs["job"]
            self.job_events[job.job_signature]["finish time"] = job.finish_time

        # When a log is submitted update also the values
        if evt == evts.JobStart or evt == evts.JobFinish:
            if self.cluster_events["checkpoints"][-1] != self.cluster.makespan:
                self.cluster_events["checkpoints"].append(self.cluster.makespan)
                self.cluster_events["unused cores"].append(self.cluster.get_idle_cores())
                if evt == evts.JobFinish:
                    self.cluster_events["finished jobs"].append(self.cluster_events["finished jobs"][-1] + 1)
            else:
                self.cluster_events["unused cores"][-1] = self.cluster.get_idle_cores()
                if evt == evts.JobFinish:
                    self.cluster_events["finished jobs"][-1] = self.cluster_events["finished jobs"][-1] + 1
 

    def setup(self):

        # Cluster wide events
        self.cluster_events = dict()
        # self.cluster_events["checkpoints"] = set()
        # self.cluster_events["checkpoints"].add(0)
        self.cluster_events["checkpoints"] = [0]
        self.cluster_events["unused cores"] = [self.cluster.total_cores]
        self.cluster_events["deploying:spread"] = 0
        self.cluster_events["deploying:exec-colocation"] = 0
        self.cluster_events["deploying:wait-colocation"] = 0
        self.cluster_events["deploying:compact"] = 0
        self.cluster_events["deploying:success"] = 0
        self.cluster_events["deploying:failed"] = 0
        self.cluster_events["finished jobs"] = [0]

        # Events #
        # Job events
        self.job_events: dict[str, dict] = dict()

        # Init job events
        for job in self.database.preloaded_queue:
            # Job events
            jevts = {
                    "trace": [], # [co-job, start time, end time]
                    "speedups": [], # [sp1, sp2, ..]
                    "cores": dict(), # {cojob1: cores1, cojob2: cores2, ..}
                    "assigned procs": ProcSet(),
                    "hosts": set(),
                    "remaining time": [],
                    "start time": 0,
                    "finish time": 0,
                    "submit time": 0,
                    "waiting time": 0,
                    "wall time": job.wall_time,
                    "num of processes": job.num_of_processes
            }
            self.job_events[job.job_signature] = jevts

    def get_gantt_representation(self):

        # Create the color palette for each job
        num_of_jobs = len(self.job_events.keys())
        jcolors = colors.sample_colorscale(self.scale, [n/(num_of_jobs - 1) for n in range(num_of_jobs)])

        # Create data for figure
        fig_data = list()

        for idx, [key, jevt] in enumerate(self.job_events.items()):

            for interval in jevt["assigned procs"].intervals():
                x_min = jevt["start time"]
                x_max = jevt["finish time"]
                y_min = interval.inf
                y_max = interval.sup

                xs = [x_min, x_max, x_max, x_min, x_min]
                ys = [y_min, y_min, y_max, y_max, y_min]

                fig_data.append(go.Scatter(
                    x=xs,
                    y=ys,
                    mode="lines",
                    legendgroup=key,
                    line=dict(width=0.1, color="black"),
                    fill="toself",
                    fillcolor=jcolors[idx],
                    showlegend=False,
                    name=f"<b>{key}</b><br>"+
                    f"submit time = {jevt['submit time']:.2f} s<br>"+
                    f"start time = {jevt['start time']:.2f} s<br>"+
                    f"finish time = {jevt['finish time']:.2f} s<br>"+
                    f"waiting time = {jevt['waiting time']:.2f} s<br>"+
                    f"hosts = {len(jevt['hosts'])}<br>"+
                    f"processors = {len(jevt['assigned procs'])}",
                ))

        xaxis_tickvals = [i * (self.cluster.makespan / 10) for i in range(0, 11)]
        xaxis_ticktext = [str(timedelta(seconds=i)).split('.')[0] for i in xaxis_tickvals]

        fig = go.Figure(data=fig_data)
        fig.update_layout(
                title=f"<b>{self.scheduler.name}</b><br>Gantt Plot",
                title_x=0.5,
                yaxis=dict(
                    title="<b>Cores</b>",
                    range=[0, self.cluster.total_cores],
                    tickmode="array",
                    tickvals=[self.cluster.total_cores],
                ),
                xaxis=dict(
                    title="<b>Time</b>",
                    tickmode="array",
                    tickvals=xaxis_tickvals,
                    ticktext=xaxis_ticktext
                ),
                template="seaborn"
        )
        return fig.to_json()

    def get_jobs_utilization(self, logger):
        """Get different utilization metrics for each job in comparison to
        another (common use: default scheduling) logger
        """

        if not isinstance(logger, Logger):
            raise RuntimeError("Provide a Logger instance")

        # Boxplot points
        points = dict()

        for job_sig in self.job_events:

            # Utilization numbers
            job_points = {
                "speedup": (logger.job_events[job_sig]["finish time"] - logger.job_events[job_sig]["start time"]) / (self.job_events[job_sig]["finish time"] -self.job_events[job_sig]["start time"]),
                "turnaround": (logger.job_events[job_sig]["finish time"] - logger.job_events[job_sig]["submit time"]) / (self.job_events[job_sig]["finish time"] -self.job_events[job_sig]["submit time"]),
                "waiting": logger.job_events[job_sig]["waiting time"] - self.job_events[job_sig]["waiting time"]
            }

            points[job_sig] = job_points

        return points

    def get_waiting_queue_graph(self):
        num_of_jobs: list[int] = list()
        for check in sorted(list(self.cluster_events["checkpoints"])):
            jobs_in_check = 0
            for _, jevt in self.job_events.items():
                if jevt["submit time"] <= check and jevt["start time"] > check:
                    jobs_in_check += 1
            num_of_jobs.append(jobs_in_check)

        return (
                sorted(list(self.cluster_events["checkpoints"])),
                num_of_jobs
        )

    def get_jobs_throughput(self):
        return (
                sorted(list(self.cluster_events["checkpoints"])),
                self.cluster_events["finished jobs"]
        )

    def get_unused_cores_graph(self):
        self.cluster_events["unused cores"].append(self.cluster.total_cores)
        return (
                sorted(list(self.cluster_events["checkpoints"])),
                self.cluster_events["unused cores"]
        )

    def get_workload(self):
        """Return 1-5 and 9 fields frm the Standart Workload Format
        """

        header = "Job Number,"
        header += "Submit Time,Wait Time,Run Time," # Actual times
        header += "Number of Allocated Processors,Average CPU Time Used,Used Memory," # Used resources
        header += "Requested Number of Processors,Requested Time,Requested Memory," # Requested resources
        header += "Status,User ID,Group ID,Executable Number," # Assign job_name
        header += "Queue Number,Partition Number,Preceding Job Number,Think Time from Preceding Job\n" # Irrelevant for us

        workload = ""
        for jevt_id, jevt in self.job_events.items():
            job_id, job_name = jevt_id.split(':')
            workload += f"{job_id},"
            workload += f"{jevt['submit time']},{jevt['waiting time']},{jevt['finish time']-jevt['start time']},"
            workload += f"{len(jevt['assigned procs'])},,,"
            workload += f"{jevt['num of processes']},{jevt['wall time']},,"
            workload += f"1,,,{job_name},"
            workload += f",,,\n"

        return header + workload
