# Quick example

1. Create your project's json file based on the available options of *project_example.jsob*.
2. Run the experiment with *python submit.py [project-file]*. It will automatically calculate
the number of ranks needed for the simulations.
3. Insided *run.py* you can get hooks from the *logger* instances to do actions (e.g. export csv for workloads).
This will be added as functionality to the project file and parser.
