import inspect
import logging
import os
import psutil
import socket
import textwrap

def get_ancestry_tree() -> list[str]:
    proc = psutil.Process()
    ancestors = [f"{proc.pid}: {proc.name()}"]
    while True:
        parent = proc.parent()
        ancestors.append(f"{parent.pid}: {parent.name()}")
        if parent.pid == 1:
            return ancestors
        else:
            proc = parent

def define_logger(log_ancestry=False, log_env=False) -> logging.Logger:

    stack_trace = inspect.stack()
    name = os.path.basename(stack_trace[1].filename).replace(".py", "")

    logger = logging.getLogger(name)

    debug_enabled = os.environ.get("ELiSE_DEBUG", "false").lower()
    if debug_enabled in ["1", "yes", "true"]:
        logger.setLevel("DEBUG")
        file_handler = logging.FileHandler(filename=f"log_{name}_{socket.gethostname()}_{os.getpid()}", mode="a", encoding="utf-8")
        file_formater = logging.Formatter(fmt="[%(levelname)s] {%(thread)s} (%(asctime)s) - %(filename)s:%(funcName)s - %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formater)
        logger.addHandler(file_handler)

        if log_ancestry:
            ancestry = "\n".join(["\n\tANCESTRY", "\t--------"] + [f"\t{proc_info}" for proc_info in get_ancestry_tree()])
            logger.info(ancestry)

        if log_env:
            env = "\n".join(["\n\tENVIRONMENT", "\t-----------"] + [f"\t{it[0]}={it[1]}" for it in os.environ.items()])
            logger.info(env)


    return logger
