# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import time

from hatchet.util import profiler
from itertools import groupby


def all_equal(iterable):
    """Returns True if all the elements are equal to each other

    Taken from the Python Itertools Recipes page:
    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    g = groupby(iterable)
    return next(g, True) and not next(g, False)


def check_distinct_graphs(th_list):
    """Take a list of objects with hatchet "graph" as an attribute and see if any of the graphs in the list match another."""
    if len(th_list) <= 1:
        return True
    for i in range(len(th_list)):
        for j in range(i + 1, len(th_list)):
            if all_equal([th_list[i].graph, th_list[j].graph]):
                return False
    return True


def are_synced(gh, df):
    """Check if node objects are equal in graph and dataframe id(graph_node) == id(df_node)."""
    for graph_node in gh.traverse():
        node_present = False
        for df_node in df.index.get_level_values("node"):
            if id(df_node) == id(graph_node):
                node_present = True
                continue
        if not node_present:
            return False
    return True


def print_graph(graph):
    """Print the nodes in a hatchet graph"""
    i = 0
    for node in graph.traverse():
        print(node + "(" + hash(node) + ") (" + id(node) + ")")
        i += 1
    return i


def write_profile(func, filepath=str(time.time() * 1e9) + ".pstats", *args, **kwargs):
    """Use hatchet profiler to profile a function and output to a file"""
    prf = profiler.Profiler()
    prf.start()
    result = func(*args, **kwargs)
    prf.stop()
    prf.print_me()
    prf.write_to_file(filepath)
    return result
