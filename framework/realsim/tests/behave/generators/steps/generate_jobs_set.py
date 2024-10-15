import behave
import sys
import os


# API
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../../../"
)))

# REALSIM
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../../"
)))

from api.loader import LoadManager
from realsim.generators.random import RandomGenerator
from realsim.generators.keysdict import KeysDictGenerator
from realsim.generators.keyslist import KeysListGenerator


@behave.given("a random generator with {jobs} jobs")
def given_random_impl(context, jobs):

    # Create the LoadManager instance for NAS in aris.compute
    lm = LoadManager("aris.compute", "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")

    # Create a random generator
    generator = RandomGenerator(lm)
    
    # Setup the context for test
    context.generator = generator
    context.jobs = int(jobs)

@behave.given("a dictionary generator")
def given_dict_impl(context):

    # Create the LoadManager instance for NAS in aris.compute
    lm = LoadManager("aris.compute", "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")

    # Create a dictionary generator
    generator = KeysDictGenerator(lm)

    # Create the query dictionary
    query = dict()
    # Create the results dictionary
    res_dict = dict()
    for row in context.table:
        load = row["load"]    
        freq = int(row["frequency"])
        query[load] = freq
        res_dict[load] = 0

    # Generate set
    jobs_set = generator.generate_jobs_set(query)

    for job in jobs_set:
        if job.load.full_load_name in list(res_dict.keys()):
            res_dict[job.load.full_load_name] += 1

    context.test_result = (res_dict == query)

    if context.test_result == False:
        print(query, res_dict)

@behave.given("a list generator")
def given_list_impl(context):
    # Create the LoadManager instance for NAS in aris.compute
    lm = LoadManager("aris.compute", "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")

    load_list = list()
    for row in context.table:
        load_list.append(row["load"])

    # Create a list generator
    generator = KeysListGenerator(lm)

    # Generate jobs set
    jobs_set = generator.generate_jobs_set(load_list)

    test_result = True
    for i, job in enumerate(jobs_set):
        test_result &= (job.load.full_load_name == load_list[i])
        if test_result == False:
            print(job, load_list[i])

    context.test_result = test_result

@behave.when("we ask for a jobs set")
def when_impl(context):

    if "Random generator" in context.scenario.name:
        test_result = True
        jobs = context.jobs
        gen = context.generator

        # Create 100 sets of jobs to test true randomness
        sets = [gen.generate_jobs_set(jobs) for _ in range(100)]

        # Test true randomness
        for jobs_set_1 in sets:
            count = 0
            for jobs_set_2 in sets:
                mid_result = jobs_set_1 == jobs_set_2
                if mid_result == True:
                    count += 1

                if count > 1:
                    test_result = False

                if test_result == False:
                    # print("ERROR:", jobs_set_1, jobs_set_2)
                    break

            if test_result == False:
                break

        context.test_result = test_result

    elif "Dictionary generator":
        pass

    elif "List generator":
        pass

    else:
        raise RuntimeError("Didn't provide a generator")

@behave.then("we get a true random set of jobs")
def then_random_impl(context):
    assert context.test_result

@behave.then("we get a random jobs set with the correct loads and their frequencies")
def then_dict_impl(context):
    assert context.test_result

@behave.then("we get a jobs set with the same loads and their placement as defined by the list")
def then_list_impl(context):
    assert context.test_result
