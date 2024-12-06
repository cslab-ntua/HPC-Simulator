import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__name__), "..", "..")
))

from realsim.generators import *
from realsim.generators.AGenerator import  AbstractGenerator


class SWFGenerator(AbstractGenerator[str]):

    name = "SWF Generator"
    description = "Generator that reads from a SWF file to produce a workload"

    def __init__(self):
        AbstractGenerator.__init__(self)
        header = "Job Number,"
        header += "Submit Time,Wait Time,Run Time," # Actual times
        header += "Number of Allocated Processors,Average CPU Time Used,Used Memory," # Used resources
        header += "Requested Number of Processors,Requested Time,Requested Memory," # Requested resources
        header += "Status,User ID,Group ID,Executable Number," # Assign job_name
        header += "Queue Number,Partition Number,Preceding Job Number,Think Time from Preceding Job" # Irrelevant for us
        self.swf_header = {name:val for val, name in enumerate(header.split(","))}


    def generate_job(self, job_record_line) -> Job:

        job_record = job_record_line.split()
        
        job = Job(
                  job_id=int(job_record[self.swf_header["Job Number"]]),
                  job_name=job_record[self.swf_header["Executable Number"]],
                  num_of_processes=int(job_record[self.swf_header["Requested Number of Processors"]]),
                  assigned_hosts=list(),
                  remaining_time=float(job_record[self.swf_header["Run Time"]]),
                  submit_time=float(job_record[self.swf_header["Submit Time"]]),
                  waiting_time=0,
                  wall_time=float(job_record[self.swf_header["Requested Time"]])
                  )

        return job

    def generate_jobs_set(self, arg: str) -> list[Job]:
        if not os.path.exists(arg):
            raise Exception("The swf file passed doesn't exist.")
        jobs_set = list()
        with open(arg, "r") as fd:
            for line in fd.readlines():
                if not line.startswith(";"):
                    jobs_set.append(self.generate_job(line))
        
        if jobs_set == list():
            raise Exception("The swf file passed was empty.")
        
        return jobs_set
