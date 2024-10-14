import subprocess
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator

if __name__ == "__main__":
    try:
        project_filename = sys.argv[1]
    except:
        raise RuntimeError("Didn't provide a project file")

    batch_creator = BatchCreator(project_filename)
    sims_num = batch_creator.get_procs_num()

    print(f"Number of sims = {sims_num}")

    process = subprocess.Popen(["mpirun", "--bind-to", "none", "--oversubscribe", "-np", str(sims_num), "python", "run.py", project_filename], 
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print(stdout.decode())
    print(stderr.decode())
