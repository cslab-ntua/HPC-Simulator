import behave
import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
    ))

from api.loader import LoadManager

@behave.given("a preloaded load manager of {machine} and {suite}")
def given_impl(context, machine, suite):
    lm = LoadManager(machine, suite)
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")
    context.lm = lm

@behave.when("we slice it")
def when_impl(context):

    context.res = True
    res = True

    for row in context.table:

        names = row["slice"].replace(' ','').split(',')
        names_exist = list(filter(
            lambda name: name in context.lm.loads, 
            names
        ))

        sliced_lm = context.lm[*names]
        sliced_names = list(sliced_lm.loads.keys())

        res &= type(sliced_lm) == LoadManager

        if not res:
            print(f"ERROR: slice of load manager {context.lm.machine} {context.lm.suite} is not of type LoadManager")
            context.result = False
            break

        if names_exist == []:

            # If no name was inside the first load manager then reutrn the
            # original load manager
            for name in list(context.lm.loads.keys()):
                res &= (name in sliced_names)
                if not res:
                    print(f"ERROR: {name} not in sliced load manager")
                    break

                res &= (context.lm(name) == sliced_lm(name))
                if not res:
                    print(f"ERROR: load instance of {name} is not equal to the instance of sliced load manager with the same name")
                    break

            if not res:
                context.res = False
                break

        else:

            # First check that the names asked that exist in the original load
            # manager are present in the sliced load manager
            for name in sliced_names:
                res &= (name in names_exist)
                if not res:
                    print(f"ERROR: {name} that exist in the sliced load manager was never asked")
                    break

            if not res:
                context.res = False
                break

            # Afterwards check if the load instances of the sliced load manager
            # are equal to the instances of the original load manager
            for name in sliced_names:
                res &= (sliced_lm(name) == context.lm(name))
                if not res:
                    print(f"ERROR: load instance of {name} in the sliced load manager is not equal to the load of the original load manager of the same name\n")

            if not res:
                context.res = False
                break

@behave.then("we get a load manager that manages the loads defined by the slice if they exist")
def then_impl(context):
    assert context.res == True
