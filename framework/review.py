import plotly.graph_objects as go
import os
import sys

import re
sys.path.append(os.path.abspath(
    os.path.dirname(__file__)
))
sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../"
    )
))

from api.loader import LoadManager
from realsim.generators.random import RandomGenerator
from realsim.generators.keysdict import KeysDictGenerator
from realsim.generators.keyslist import KeysListGenerator

from realsim.cluster.shallow import ClusterShallow
from realsim.scheduler.compact import CompactScheduler
from realsim.logger.logger import Logger

from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.scheduler.balancerFullOn import BalancerFullOn

# Create a load manager and import loads from database
lm = LoadManager("aris.compute", "NAS")
lm.import_from_db(username="admin", password="admin", dbname="storehouse")
try:
    lm.loads.pop("613.soma_s.1024")
except:
    pass
try:
    lm.loads.pop("cg.E.2048")
except:
    pass

lm.loads.pop("bt.E.2025")
lm.loads.pop("sp.E.2025")
# lm.loads.pop("cg.E.2048")
lm.loads.pop("ft.E.2048")
lm.loads.pop("lu.E.2048")

klist_gen = KeysListGenerator(lm)

file = sys.argv[1]
fd = open(file, "r")
loads_names = list()
for line in fd.readlines():
    loads_names.append(line.replace("\n", ""))

speedups = list()
boxpoints = list()
exps = 1
for _ in range(exps):

    # Generate a job set
    jobs_set = klist_gen.generate_jobs_set(loads_names)

    # COMPACT
    cluster = ClusterShallow(426, 20)
    cluster.deploy_to_waiting_queue(jobs_set)
    compact = CompactScheduler()
    logger = Logger()

    cluster.assign_scheduler(compact)
    compact.assign_cluster(cluster)
    cluster.assign_logger(logger)
    compact.assign_logger(logger)
    cluster.run()

    logger.plot_resources()

    # FULLON
    cluster_fullon = ClusterExhaustive(426, 20)
    cluster_fullon.deploy_to_waiting_queue(jobs_set)

    balancer_fullon = BalancerFullOn()

    logger_fullon = Logger()

    cluster_fullon.assign_scheduler(balancer_fullon)
    balancer_fullon.assign_cluster(cluster_fullon)

    cluster_fullon.assign_logger(logger_fullon)
    balancer_fullon.assign_logger(logger)

    cluster_fullon.run()

    # print( logger_fullon.job_events )

    logger_fullon.plot_resources()

    boxpoints.append(
            logger_fullon.jobs_speedup_boxpoints(logger)
    )

    speedup = cluster.makespan / cluster_fullon.makespan
    speedups.append(speedup)

fig = go.Figure()

for i, points in enumerate(boxpoints):
    fig.add_trace(
            go.Box(
                y=points,
                x=[i]*len(points),
                name=f"Exp{i}",
                x0=f"Exp{i}",
                boxpoints="all",
                showlegend=False,
                boxmean="sd"
            )
    )

fig.add_trace(
        go.Scatter(x=list(range(len(speedups))), 
                   y=speedups, mode="lines+markers",
                   marker=dict(color="black"), name="Experiments' Speedup"
        )
)

fig.add_hline(y=1, line_color="black", line_dash="dot")

fig.update_layout(
        title=f"<b>Jobs' speedups</b>",
        title_x=0.5,
        xaxis=dict(
            title="<b>Experiments</b>",
            tickmode="array",
            tickvals=list( range(exps) ),
            ticktext=[f"Exp{i}" for i in range(exps)]
        ),
        yaxis=dict(title="<b>Speedup</b>")
)

fig.show()
