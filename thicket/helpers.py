import time

from hatchet.util import profiler
from itertools import groupby


def print_graph(graph):
    """Print the nodes in a hatchet graph"""
    i = 0
    for node in graph.traverse():
        print(f"{node} {hash(node)}")
        i += 1
    return i


def write_profile(func, filepath=f"{time.time_ns()}.pstats", *args, **kwargs):
    """Use hatchet profiler to profile a function and output to a file"""
    prf = profiler.Profiler()
    prf.start()
    result = func(*args, **kwargs)
    prf.stop()
    prf.print_me()
    prf.write_to_file(filepath)
    return result


def all_equal(iterable):
    """Returns True if all the elements are equal to each other

    Taken from the Python Itertools Recipes page:
    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    g = groupby(iterable)
    return next(g, True) and not next(g, False)


def check_distinct_graphs(th_list):
    """Take a list of objects with hatchet "graph" as an attribute and see if any of the graphs in the list match another."""
    if (len(th_list) <= 1):
        return True
    for i in range(len(th_list)):
        for j in range(i+1, len(th_list)):
            if all_equal([th_list[i].graph, th_list[j].graph]):
                return False
    return True
