# What is the simulator

The simulator we created is a statistical representation of the execution of a
workload under resource limits of an HPC system with different scheduling
algorithms. The workload is built upon real experiment logs under a specific HPC
infrastructure. For the simulator to produce reliable results the logs that
build the workload and the specifications of the cluster must be from the same
HPC system. The schedulers are algorithms for job placement and resource
handlers under the restraints and limitation of said simulated cluster. The main
attraction of the simulator is the ability for rapid development of scheduling
algorithms.

# How is it different from others

The computation and communication of each job are not simulated. It works under 
the premise that these values are obtained by the real experiment logs and these 
reflect the infrastructure they ran on. What is simulated is the execution of
the workload under different scheduling algorithms by varying conditions and
limitations.

Another major difference is that the simulator is employed with a model for
co-allocated execution. A user can also provide their own analytical or
stochastic model with pretty ease.

# How is co-scheduling supported

![The simulation loop with events and steps](./assets/SimulationLoop_Events_Steps.svg)

The simulator computes the remaining time of each running job inside steps. The
steps are intervals between significant events. Such events are, job submission,
the deployment of jobs for execution, the
termination of a job and others. At each step and for each job the simulator scans
the hosts the job has allocated processes on and finds the neighboring jobs 
co-executing - sharing resources.

At each step, the simulator scans the assigned hosts for each job, and finds its
neighbors - jobs sharing resources with it. For each neighbor, the possible
speedups for the job are calculated. From them, the minimum speedup, 
will be the whole job's speedup for the current step. 
The remaining execution time for the job is calculated by multiplying the
current remaining time with the ratio of the old speedup, the speedup calculated
by the previous step, to the new speedup.
These calculations are mathematically expressed as following:

$speedup_{n+1} = min_{S}(\forall neighbor: \ job.hosts \cap neighbor.hosts \neq \emptyset. (S = getSpeedup(job, neighbor)))$

and

$job.remTime_{n+1} = job.remTime_{n} \times \frac{speedup_{n}}{speedup_{n+1}}$

The initial speedup for each job is defined as 1 because the compact execution
time for the job is used. In the second formula, the remaining execution time of
the job is multiplied with the speedup from the previous step. This computes
the remaining portion of time for the job if it was executing under the compact
allocation policy. Then this portion is divided by the speedup of the new step
to get the current remaining execution time of the job under a co-located
policy.

The following picture depicts a possible co-execution among the jobs A, B, C and D.
It is a snapshot of a step in the simulation. Each box represents a CPU in the simulated cluster. 
![Jobs co-allocated among CPUs](./assets/JobsHosts.svg)

1. For Job A, the speedup for the current simulation step is calculated as 
the worst speedup between the co-executing jobs B and C, $speedup_A = min(speedup_{A \ with \ B}, speedup_{A \ with \ C})$.
2. For Job B, because it is co-executing alongside only Job A, the speedup for
   the current step is $speedup_B = speedup_{B \ with \ A}$.
3. Job C is sharing resources with jobs A and D, so the speedup is $speedup_C = min(speedup_{C \ with \ A}, speedup_{C \ with \ D})$.
4. Job D is co-allocated only with Job C, so the speedup is $speedup_D = speedup_{D \ with \ C}$.

# What are the advantages

1. Simplicity in the design architecture leads to better understanding,
   maintainability and extensibility.
2. A distributed MVC architecture which is easy to be expanded by other
   researchers. It helps in rapid development and testing for simulation
   algorithms.
3. Optimized to produce fast results for large simulations. Even a low
   performance computer can run a simulation of many jobs.
4. The simulations are scalable. A simulation is roughly represented as a
   workload and a scheduler. The framework provides the ability to distribute
   the work as workloads and one scheduler (many-to-1), a workload with many 
   schedulers (1-to-many) and workloads with many schedulers (many-to-many).
5. The simulator has a GUI for ease of use.
5. It provides extensive reports and plots for a simulation without additional
   coding needed.
6. It is a standalone software solution for HPC simulations. There are no third
   party applications required.


# What are the disadvatages

1. No real computation and communication simulations for the jobs.
2. Doesn't fully support artificial workloads. The workloads must be built upon
   real experiment logs that reflect the HPC system that ran on.
