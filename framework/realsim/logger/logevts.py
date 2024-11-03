from datetime import timedelta
from abc import ABC, abstractmethod

class LogEvent:

    hook = ""

    @staticmethod
    def _log(msg: str, sec: float) -> str:
        timestamp = f"{timedelta(seconds=sec)}"
        return f"({timestamp})    {msg}"

    @staticmethod
    def log(extra: str, sec: float) -> str:
        return ""



class JobStart(LogEvent):
    hook = "job_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Job started executing [{extra}]", sec)

class JobFinish(LogEvent):
    hook = "job_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Job finished execution [{extra}]", sec)

class JobDeployedToHost(LogEvent):
    hook = "cluster_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Job deployed to host [{extra}]", sec)

class JobCleanedFromHost(LogEvent):
    hook = "cluster_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Job cleaned from host [{extra}]", sec)

class CompEngineNextTimeStep(LogEvent):
    hook = "compeng_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Calculated the simulation time step [{extra}]", sec)

class CompEngineJobsRemTime(LogEvent):
    hook = "compeng_logs"
    @staticmethod
    def log(extra: str, sec: float) -> str:
        return LogEvent._log(f"Calculated the remaining time of jobs", sec)
