# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd


def _are_synced(gh, df):
    """Check if node objects are equal in graph and dataframe id(graph_node) ==
    id(df_node).
    """
    for graph_node in gh.traverse():
        node_present = False
        for df_node in df.index.get_level_values("node"):
            if id(df_node) == id(graph_node):
                node_present = True
                continue
        if not node_present:
            return False
    return True


def _missing_nodes_to_list(a_df, b_df):
    """Get a list of node differences between two dataframes. Mainly used for "tree"
    function.

    Arguments:
        a_df (Dataframe): First pandas Dataframe
        b_df (Dataframe): Second pandas Dataframe

    Returns:
        (list): List of numbers in range (0, 1, 2). "0" means node is in both, "1" is
            only in "a", "2" is only in "b"
    """
    missing_nodes = []
    a_list = list(map(hash, list(a_df.index.get_level_values("node"))))
    b_list = list(map(hash, list(b_df.index.get_level_values("node"))))
    # Basic cases
    while a_list and b_list:
        a = a_list.pop(0)
        b = b_list.pop(0)
        while a > b and b_list:
            missing_nodes.append(2)
            b = b_list.pop(0)
        while b > a and a_list:
            missing_nodes.append(1)
            a = a_list.pop(0)
        if a == b:
            missing_nodes.append(0)
            continue
        elif a > b:  # Case where last two nodes and "a" is missing "b" then opposite
            missing_nodes.append(2)
            missing_nodes.append(1)
        elif b > a:  # Case where last two nodes and "b" is missing "a" then opposite
            missing_nodes.append(1)
            missing_nodes.append(2)
    while a_list:  # In case "a" has a lot more nodes than "b"
        missing_nodes.append(1)
        a = a_list.pop(0)
    while b_list:  # In case "b" has a lot more nodes than "a"
        missing_nodes.append(2)
        b = b_list.pop(0)
    return missing_nodes


def _new_statsframe_df(df, multiindex=False):
    """Generate new aggregated statistics table from a dataframe. This is most commonly
    needed when changes are made to the performance data table's index.

    Arguments:
        df (DataFrame): Input dataframe to generate the aggregated statistics table from
        multiindex (Bool, optional): Option to setup MultiIndex column structure. This
            is standard to do if performance data table is MultiIndex.

    Returns:
        (DataFrame): new aggregated statistics table
    """
    nodes = list(set(df.reset_index()["node"]))  # List of nodes
    names = [node.frame["name"] for node in nodes]  # List of names

    # Create new dataframe with "node" index and "name" data by default.
    new_df = pd.DataFrame(
        data={"node": nodes, "name": names},
    ).set_index("node")

    # Create MultiIndex structure if necessary.
    if multiindex:
        new_df.columns = pd.MultiIndex.from_tuples([("", "name")])

    return new_df


def _print_graph(graph):
    """Print the nodes in a hatchet graph"""
    i = 0
    for node in graph.traverse():
        print(f"{node} ({hash(node)}) ({id(node)})")
        i += 1
    return i


def _resolve_missing_indicies(th_list):
    """Resolve indices if at least 1 profile has an index that another doesn't.

    If at least one profile has an index that another doesn't, then issues will arise
    when unifying. Need to add this index to other thickets.

    Note that the value to use for the new index is set to '0' for ease-of-use, but
    something like 'NaN' may arguably provide more clarity.
    """
    # Create a set of all index possibilities
    idx_set = set({})
    for th in th_list:
        idx_set.update(th.dataframe.index.names)

    # Apply missing indicies to thickets
    for th in th_list:
        for idx in idx_set:
            if idx not in th.dataframe.index.names:
                print(f"Resolving '{idx}' in thicket: ({id(th)})")
                th.dataframe[idx] = 0
                th.dataframe.set_index(idx, append=True, inplace=True)


def _sync_nodes(gh, df):
    """Set the node objects to be equal in both the graph and the dataframe.

    id(graph_node) == id(df_node) after this function for nodes with equivalent hatchet
    nid's.
    """
    index_names = df.index.names
    df.reset_index(inplace=True)
    for graph_node in gh.traverse():
        df["node"] = df["node"].apply(
            lambda df_node: graph_node
            if (hash(graph_node) == hash(df_node))
            else df_node
        )
    df.set_index(index_names, inplace=True)


def _sync_nodes_frame(gh, df):
    """Update dataframe node objects hnid's based off their positioning relative to a
    given graph.

    id(graph_node) == id(df_node) after this function for nodes with equivalent hatchet
    nid's.

    TODO: This function may be superior to _sync_nodes and may be able to replace it.
    Need to investigate.
    """
    assert df.index.nlevels == 2  # For num_profiles assumption

    # TODO: Graph function to list conversion: move to Hatchet?
    gh_node_list = []
    for gh_node in gh.traverse():
        gh_node_list.append(gh_node)

    num_profiles = len(df.groupby(level=1))
    index_names = df.index.names
    df.reset_index(inplace=True)
    df_node_list = df["node"][::num_profiles].to_list()

    # Sequentially walk through graph and dataframe and modify dataframe hnid's based off graph equivalent
    i = 0
    j = 0
    while i < len(gh_node_list) and j < len(df_node_list):
        if gh_node_list[i].frame == df_node_list[j].frame:
            df_node_list[j] = gh_node_list[i]
            j += 1
        else:
            i += 1

    # Extend list to match multi-index dataframe structure
    df_list_full = []
    for node in df_node_list:
        temp = []
        for idx in range(num_profiles):
            temp.append(node)
        df_list_full.extend(temp)
    # Update nodes in the dataframe
    df["node"] = df_list_full

    df.set_index(index_names, inplace=True)


def _get_perf_columns(df):
    """Get list of performance dataframe columns that are numeric.

    Numeric columns can be used with thicket's statistical functions.

    Arguments:
        df (DataFrame): thicket dataframe object

    Returns:
        numeric_columns (list): list of numeric columns
    """
    numeric_types = ["int32", "int64", "float32", "float64"]

    numeric_columns = df.select_dtypes(include=numeric_types).columns.tolist()

    # thicket object without columnar index
    if df.columns.nlevels == 1:
        if "nid" in numeric_columns:
            numeric_columns.remove("nid")

        return numeric_columns
    # columnar joined thicket object
    else:
        return [x for x in numeric_columns if "nid" not in x]
