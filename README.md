![HPC Simulator logo](./assets/HPCSim.png)

## Setting up the environment using conda

```bash
# Install the necessary dependencies
conda env create -f environment.yml

# Starting the environment
conda activate hpc-sim
```

## Running the dashboard app

```bash
# Redirect to the following directory
cd framework/dashboard

# Start the app
python dashboard.py
```

The app currently runs at debug mode provided by the Dash framework, because
this is a work in progress application. It can be easily changed inside the
*dashboard.py* file.

## The app's directory structure
- **api** includes the necessary package **loader** for communicating with
  the database and parsing of the stored (work)loads.
- **framework** includes a **new** fast prototyping front-end and the main
  backbone of the simulator.
    - **dashboard** includes the front-end dashboard application for the
      simulation framework. It will be mainly used for fast experimentation on
      different data and scheduling algorithms.
    - **realsim** is the main python library that includes all the *core
      components* of the simulation framework. Further information and
      instructions (for users and developers) can be found by navigating through
      each of the packages (jobs, generators, cluster, scheduler, logger).
