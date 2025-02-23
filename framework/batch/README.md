Schematic File
=====================

## Definition

- File in YAML format to configure the simulation
- Defines the workloads, schedulers and post processing actions

## Format

```yaml
---
name: "Name of the project"
description: "[optional] Description of the project"
# Section for defining workloads (preferable unamed)
workloads:
  # dir > load-manager > db is the importance hierarchy
  # when deciding where to look for the logs
  - dir: "Path to directory of compact and coschedules"
    load-manager: "Path to a pickled and baked LoadManager instance to build jobs from"
    db: "Mongo database url"
    loads-machine: "Name of machine"
    loads-suite: "Name of suite"
    heatmap: "[optional] Using a user specified heatmap (csv format) to overload the heatmap produced by a `load-manager` or `db` instances"
    generator:
      type: "Type of generator (or a path to a python file)"
      arg: "Argument (for Random the number of jobs, for Dict the name and frequency of loads and for List the path to the file containing the list)"
      distribution: "[optional] overrides submit time of jobs based on a distribution"
        type: "Type of the distribution or path to .py file for submit times"
        arg: "Argument to pass to distribution"
    cluster:
      nodes: "Number (int) of nodes in a cluster"
      socket-conf: "The configuration of sockets in a node. Should be a list of ints"
    repeat: "Number (int) of how many times this workload will repeat"
# Section for defining schedulers and their options
schedulers:
  default: "Set the default scheduler name or .py file for the simulation"
  others:
    - "List of scheduler names"
    - "or .py files"
  gloabl_options:
    attr0: "Set of attributes and their values to be passed to all the schedulers"
    attr1: "example"
# Section for defining after simulation actions (based on Logger's api)
# Only get_gantt_representation and get_workload are currently available
actions:
  get_gantt_representation:
    workloads: "all or list of numbers representing workloads"
    schedulers: "all or list with names of schedulers"
    arg: "extra arguments to pass to this action"
  get_workload:
    workloads: "all or list of numbers representing workloads"
    schedulers: "all or list with names of schedulers"
    arg: "extra arguments to pass to this action"
...
```

## Example

```yaml
---
name: "Toy schematic"
workloads:
  # Workload based on a Mongo database
  - &default_workload
    db: "mongodb+srv://credentials@dbname.hostname.mongodb.net"
    loads-machine: "machine name"
    loads-suite: "suite name"
    # 100 randomly generated jobs
    generator:
      type: "Random Generator"
      arg: 100
    # A cluster of 8 nodes with 2 NUMA nodes and 12 cores each
    cluster:
      nodes: 8
      socket-conf: [12,12]
    # Generate 10 similar workloads based on the above configuration
    repeat: 10
schedulers:
  default: "EASY Scheduler"
  others:
    - "FIFO Scheduler"
    - "Conservative Scheduler"
    - "Random Ranks Co-Scheduler"
    - "<ELiSE home dir>/framework/realsim/scheduler/coschedulers/CustomCoScheduler.py"
  general-options:
    backfill_enabled: true
    compact_fallback: true
actions:
  get_workload:
    workloads: "all"
    schedulers: "all"
    workload_dir: "./csvs"
...

```

Submit Simulations with HPC-Simulator
=====================================

This script is used to submit multiple simulation runs in parallel on localhost or in an HPC environment.
The scripts automatically handles the allocation of resources depending on the environment under which it is launched.

## Usage

To use this script, you need to provide a schematic file. You can also provide a parallelizing provider. The available providers are currently:
- "mpi" (Tested and validated only with OpenMPI)
- "mp" (Python's multiprocessing library) which is the default provider if no provider is specified.

The flag --export_reports can be used to export reports for each simulation run in a csv file.

```bash
python submit.py -f <schematic_file> [-p <provider>] [--export_reports]
```

## Providers

The Python's multiprocessing library is the default provider. It can only be used on localhost.

The MPI provider is tested and validated only with OpenMPI. It can be used on both localhost and HPC environments.
There are future considerations of using more MPI providers based on what is supported by mpi4py. The next item in the list is to test it with IntelMPI.

There is also work being done to support the debugging features that these MPI providers offer.

## Schedulers

Currently the script is tested and validated only with the SLURM scheduler. There will be future considerations of supporting other schedulers starting from non-commercial solutions.

### Examples

#### Example 1: Run simulations in parallel using MPI

```bash
python submit.py -f my_schematic.yaml -p mpi
```

This will launch multiple simulation runs in parallel using MPI on localhost or HPC environment.

#### Example 2: Run simulations in parallel using Python's multiprocessing library

```bash
python submit.py -f my_schematic.yaml -p mp
```

This will launch multiple simulation runs in parallel using Python's multiprocessing library.

#### Example 3: Export reports for each simulation run (requires export_reports=True)

```bash
python submit.py -f my_schematic.yaml -p mpi --export_reports
```

This will launch multiple simulation runs in parallel using MPI and export a report file for all simulation runs.