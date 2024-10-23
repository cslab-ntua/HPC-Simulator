from json import loads as json_loads
from pickle import load as pickle_load
from functools import reduce
import importlib.util
import inspect
import os
import sys

# Introduce path to realsim
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../")
))

# Introduce path to api.loader
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
))

# LoadManager
from api.loader import LoadManager

# Import deepcopy capabilities
from realsim.jobs.utils import deepcopy_list
from copy import deepcopy

# Database
from realsim.database import Database

# Generators
from realsim.generators.abstract import AbstractGenerator
from realsim.generators.random import RandomGenerator
from realsim.generators.keysdict import KeysDictGenerator
from realsim.generators.keyslist import KeysListGenerator

# Distributions
from realsim.generators.distribution.idistribution import IDistribution
from realsim.generators.distribution.constantdistr import ConstantDistribution
from realsim.generators.distribution.randomdistr import RandomDistribution
from realsim.generators.distribution.poissondistr import PoissonDistribution

# Cluster
from realsim.cluster.cluster import Cluster

# Schedulers
from realsim.scheduler.scheduler import Scheduler
from realsim.scheduler.schedulers.fifo import FIFOScheduler
from realsim.scheduler.schedulers.easy import EASYScheduler
from realsim.scheduler.schedulers.conservative import ConservativeScheduler
from realsim.scheduler.coschedulers.ranks.random import RandomRanksCoscheduler

# Logger
from realsim.logger.logger import Logger

# ComputeEngine
from realsim.compengine import ComputeEngine
from realsim.par_compengine import ParallelComputeEngine


class BatchCreator:

    def __init__(self, path_to_script: str):

        # Ready to use generators implementing the AbstractGenerator interface
        self.__impl_generators = {
            RandomGenerator.name: RandomGenerator,
            KeysDictGenerator.name: KeysDictGenerator,
            KeysListGenerator.name: KeysListGenerator
        }

        # Ready to use schedulers implementing the Distribution interface
        self.__impl_distributions = {
            "Constant": ConstantDistribution,
            "Random": RandomDistribution,
            "Poisson": PoissonDistribution
        }

        # Ready to use schedulers implementing the Scheduler interface
        self.__impl_schedulers = {
            FIFOScheduler.name: FIFOScheduler,
            EASYScheduler.name: EASYScheduler,
            ConservativeScheduler.name: ConservativeScheduler,
            RandomRanksCoscheduler.name: RandomRanksCoscheduler
        }
        
        # Load the configuration file
        with open(path_to_script, "r") as fd:
            data = json_loads(fd.read())
            
            if "name" not in data or "workloads" not in data or "schedulers" not in data:
                raise RuntimeError("The configuration file is not properly designed")

            self.__project_name = data["name"]
            self.__project_workloads = data["workloads"]
            self.__project_schedulers = data["schedulers"]
            self.__project_actions = data["actions"] if "actions" in data else dict()

    @staticmethod
    def import_module(path):
        mod_name = os.path.basename(path).replace(".py", "")
        spec = importlib.util.spec_from_file_location(mod_name, path)
        gen_mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = gen_mod
        spec.loader.exec_module(gen_mod)
        return spec.name

    def get_procs_num(self) -> int:
        workloads_num = 0
        for workload in self.__project_workloads:
            workloads_num += 1 if "repeat" not in workload else int(workload["repeat"])

        return workloads_num * (1 + len(self.__project_schedulers["others"]))

    def process_workloads(self) -> None:

        # Process the workloads
        self.__workloads = list()

        for workload in self.__project_workloads:
        
            # Create a LoadManager based on the options given
            lm = LoadManager(machine=workload["loads-machine"],
                             suite=workload["loads-suite"])
            # A LoadManager instance can be created using
            if "path" in workload:
                # A path to a directory with the real logs
                path = workload["path"]
                lm.init_loads(runs_dir=path)
            elif "load-manager" in workload:
                # A pickled LoadManager instance (or json WIP)
                with open(workload["load-manager"], "wb") as fd:
                    lm = pickle_load(fd)
            elif "db" in workload:
                # A mongo database url
                lm.import_from_db(host=workload["db"], dbname="storehouse")
            else:
                raise RuntimeError("Couldn't provide a way to create a LoadManager")

            # Create a heatmap from the LoadManager instance or use a user-defined
            # if a path is provided
            if "heatmap" in workload:
                with open(workload["heatmap"], "r") as fd:
                    heatmap = json_loads(fd.read())
            else:
                heatmap = lm.export_heatmap()

            # Create the workload using the generator provided
            if "generator" in workload:
                generator = workload["generator"]
                gen_type = generator["type"]
                gen_arg = generator["arg"]

                # If a python file is provided for the generator
                if os.path.exists(gen_type) and ".py" in gen_type:
                    # Import generator module
                    spec_name = BatchCreator.import_module(gen_type)
                    gen_mod = sys.modules[spec_name]
                    # Get the generator class from the module
                    classes = inspect.getmembers(gen_mod, inspect.isclass)
                    # It must be a concrete class implementing the AbstractGenerator interface
                    classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], AbstractGenerator), classes))

                    # If there are multiple then inform the user that the first will be used
                    if len(classes) > 1:
                        print(f"Multiple generator definitions were found. Using the first definition: {classes[0][0]}")

                    _, gen_cls = classes[0]
                else:
                    try:
                        gen_cls = self.__impl_generators[gen_type]
                    except:
                        raise RuntimeError(f"The name {gen_type} of the generator provided does not exist")

                # Create instance of generator
                gen_inst = gen_cls(load_manager=lm)

                if "repeat" in workload:
                    repeat = int(workload["repeat"])
                else:
                    repeat = 1

                for _ in range(repeat):

                    # Generate the workload
                    gen_workload = gen_inst.generate_jobs_set(gen_arg)

                    # Check if a transformer distribution is provided by the user
                    if "distribution" in generator:
                        distribution = generator["distribution"]
                        distr_type = distribution["type"]
                        distr_arg = distribution["arg"]

                        # If a path is provided for the distribution transformer
                        if os.path.exists(distr_type) and ".py" in distr_type:
                            spec_name = BatchCreator.import_module(distr_type)
                            distr_mod = sys.modules[spec_name]
                            classes = inspect.getmembers(distr_mod, inspect.isclass)
                            classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], IDistribution), classes))
                            # If there are multiple then inform the user that the first will be used
                            if len(classes) > 1:
                                print(f"Multiple distribution definitions were found. Using the first definition: {classes[0][0]}")

                            _, distr_cls = classes[0]
                        else:
                            try:
                                distr_cls = self.__impl_distributions[distr_type]
                            except:
                                raise RuntimeError(f"Distribution of type {distr_type} does not exist")

                        distr_inst = distr_cls()
                        distr_inst.apply_distribution(gen_workload, time_step=distr_arg)

                    nodes = int(workload["cluster"]["nodes"])
                    socket_conf = tuple(workload["cluster"]["socket-conf"])
                    self.__workloads.append((gen_workload, heatmap, nodes, socket_conf))

            else:
                raise RuntimeError("A generator was not provided")

    def process_schedulers(self) -> None:

        # Process the schedulers
        # The first one in the list will always be the default
        self.__schedulers = list()

        for scheduler in [self.__project_schedulers["default"]] + self.__project_schedulers["others"]:

            if os.path.exists(scheduler) and ".py" in scheduler:
                spec_name = BatchCreator.import_module(scheduler)
                sched_mod = sys.modules[spec_name]
                classes = inspect.getmembers(sched_mod, inspect.isclass)
                classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], Scheduler), classes))
                # If there are multiple then inform the user that the first will be used
                if len(classes) > 1:
                    print(f"Multiple scheduler definitions were found. Using the first definition: {classes[0][0]}")

                _, sched_cls = classes[0]
            else:
                try:
                    sched_cls = self.__impl_schedulers[scheduler]
                except:
                    raise RuntimeError(f"Scheduler of type {scheduler} does not exist")

            self.__schedulers.append(sched_cls)

    def process_actions(self) -> None:
        """
        The structure of self.__actions
        actions = {
            workload0 = {
                scheduler0 = [],
                scheduler1 = [],
                ..
                schedulerM = []
            },
            ..
            workloadN = {
                scheduler0 = []
                scheduler1 = []
                ..
                schedulerM = []
            }
        }
        The structure of self.__extra_features is a list of (arg: str, val: T) tuples
        self.__extra_features = [(arg0, val0), (arg1, val1), ...]
        """
        # Define __actions
        self.__actions = dict()
        for i in range(len(self.__workloads)):
            workload_dict = dict()
            for sched_name in [self.__project_schedulers["default"]] + self.__project_schedulers["others"]:
                workload_dict.update({sched_name: []})
            self.__actions.update({i: workload_dict})

        # Define __extra_features
        self.__extra_features: list[tuple] = list()

        for action in self.__project_actions:
            action_workloads = self.__project_actions[action]["workloads"]
            action_schedulers = self.__project_actions[action]["schedulers"]

            action_extra_features = [(arg, val) 
                                     for arg, val in self.__project_actions[action].items() 
                                     if arg not in ["workloads", "schedulers"]]
            # Simple implementation is to overwrite an argument with the latest
            # value provided in the project file
            self.__extra_features.extend(action_extra_features)

            if action_workloads == "all":
                for workload_dict in self.__actions.values():
                    if action_schedulers == "all":
                        for sched_dict in workload_dict.values():
                            sched_dict.append(action)
                    else:
                        for sched_name in action_schedulers:
                            workload_dict[sched_name].append(action)
            else:
                for i in action_workloads:
                    if action_schedulers == "all":
                        for sched_dict in self.__actions[i].values():
                            sched_dict.append(action)
                    else:
                        for sched_name in action_schedulers:
                            self.__actions[i][sched_name].append(action)

    def create_ranks(self) -> None:
        self.process_workloads()
        self.process_schedulers()
        self.process_actions()

        # Create the ranks
        self.ranks = list()
        for idx, [workload, heatmap, nodes, socket_conf] in enumerate(self.__workloads):
            for sched_cls in self.__schedulers:

                # Create a database instance
                database = Database(workload, heatmap)
                database.setup()

                # Create a cluster instance
                cluster = Cluster(nodes, socket_conf)

                # Create a scheduler instance
                scheduler = sched_cls()
                scheduler.backfill_enabled = self.__project_schedulers["backfill_enabled"]

                # Create a logger instance
                logger = Logger(debug=False)

                # Create a compute engine instance
                compengine = ComputeEngine(database, cluster, scheduler, logger)
                compengine.setup_preloaded_jobs()
                # compengine = ParallelComputeEngine().set_db(database).set_cluster(cluster).set_scheduler(scheduler).set_logger(logger).setup()
                # compengine.setup_preloaded_jobs()

                # Set actions for this simulation
                actions = self.__actions[idx][sched_cls.name]
                
                self.ranks.append((idx, database, cluster, scheduler, logger, compengine, actions, self.__extra_features))

        print("Processed ranks")
