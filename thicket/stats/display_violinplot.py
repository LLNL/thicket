# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
import hatchet as ht
import plotly.express as px
import numpy as np

from .percentiles import percentiles
from ..utils import verify_thicket_structures

def display_violinplot(thicket, nodes=[], columns=[], **kwargs):
    """Display a violinplot for each user passed node(s) and column(s). The passed nodes
    and columns must be from the performance data table.

    Designed to take in a thicket, and display a plot with one or more violinplots
    depending on the number of nodes and columns passed.

    Arguments:
        thicket (thicket): Thicket object
        nodes (list): List of nodes to view on the x-axis
        column (list): List of hardware/timing metrics to view on the y-axis. Note, if
            using a columnar joined thicket a list of tuples must be passed in with the
            format (column index, column name).

    Returns:
        (matplotlib Axes): Object for managing violinplot.
    """

    for node in nodes:
        if not isinstance(node, ht.node.Node):
            raise ValueError(
                "Value(s) passed to node argument must be of type hatchet.node.Node."
            )

    verify_thicket_structures(thicket.dataframe, index=["node"], columns=columns)

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        df = pd.melt(
            thicket.dataframe.reset_index(),
            id_vars=["node", "name"],
            value_vars=columns,
            var_name="Performance counter",
            value_name=" ",
        )

        position = []
        for node in nodes:
            idx = df.index[df["node"] == node]
            for pos in idx:
                position.append(pos)

        # rename columns such that the x-axis label is "node" and not "name", tick marks
        # will be node names
        filtered_df = df.loc[position].rename(
            columns={"node": "hatchet node", "name": "node"}
        )

        if len(columns) > 1:
            return sns.violinplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else:
            return sns.violinplot(data=filtered_df, x="node", y=" ", **kwargs)
    # columnar joined thicket object
    else:

        """
            New code that allows for columns to be selected from different indexes of a columnar joined thicket
        """
        def column_name_mapper(current_cols):
            if current_cols[0] in ["node", "name"]:
                return current_cols[0]

            return str(current_cols)

        cols = [str(c) for c in columns]
        df_subset = thicket.dataframe[[("name", ""), *columns]].reset_index()
        df_subset.columns = df_subset.columns.to_flat_index().map(column_name_mapper)
        df_subset["name"] = thicket.dataframe["name"].tolist()
        # End new code
        
        df = pd.melt(
            df_subset,
            id_vars=["node", "name"],
            value_vars=cols,
            var_name="Performance counter",
            value_name=" ",
        )

        position = []
        for node in nodes:
            idx = df.index[df["node"] == node]
            for pos in idx:
                position.append(pos)

        # rename columns such that the x-axis label is "node" and not "name", tick marks
        # will be node names
        filtered_df = df.loc[position].rename(
            columns={"node": "hatchet node", "name": "node"}
        )
        if len(columns) > 1:
            return sns.violinplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            ), filtered_df
        else:
            return sns.violinplot(data=filtered_df, x="node", y=" ", **kwargs)


def display_violinplot_thicket(thicket_dictionary, nodes=None, columns=None, **kwargs):
    """Display a boxplot for each user passed node(s) and column(s). The passed nodes
    and columns must be from the performance data table.

    Designed to take in a thicket, and display a plot with one or more boxplots
    depending on the number of nodes and columns passed.

    Arguments:
        thicket (thicket): Thicket object
        nodes (list): List of nodes to view on the x-axis
        column (list): List of hardware/timing metrics to view on the y-axis. Note, if
            using a columnar joined thicket a list of tuples must be passed in with the
            format (column index, column name).

    Returns:
        (matplotlib Axes): Object for managing boxplot.
    """

    def column_name_mapper(current_cols):
        if current_cols[0] in ["node", "name"]:
            return current_cols[0]

        return str(current_cols)

    if nodes is None:
        raise ValueError("Nodes must be a list of lists, specifying nodes for each Thicket passed in")

    if columns is None:
        raise ValueError("Columns must be a list of lists, specifying columns for each Thicket passed in")

    #Ensures that both nodes and columns is a list of lists
    if isinstance(nodes, list) == False or all(isinstance(n, list) for n in nodes) == False:
        raise ValueError("Nodes must be a list of lists, specifying nodes for each Thicket passed in")
    
    if isinstance(columns, list) == False or all(isinstance(c, list) for c in columns) == False:
        raise ValueError("Nodes must be a list of lists, specifying nodes for each Thicket passed in")
    
    #Ensures that each Thicket has corresponding nodes, and columns. Otherwise throw an error
    if len(thicket_dictionary) != len(columns):
        raise ValueError("Length of columns does not match number of thickets")
    if len(thicket_dictionary) != len(nodes):
        raise ValueError("Length of nodes does not match number of thickets")

    #Verify that each node in the nodes lists are node types
    for node_list in nodes:
        if not all(isinstance(n, ht.node.Node) for n in node_list):
            raise ValueError(
                "Value(s) passed to node argument must be of type hatchet.node.Node."
            )

    for idx, thicket in enumerate(thicket_dictionary.items()):
        verify_thicket_structures(thicket[1].dataframe, index=["node"], columns=columns[idx])
    
    filtered_dfs = [None] * len(thicket_dictionary) #Holds the final dataframe for each thicket that we will plot
    sub_dataframes = [None] * len(thicket_dictionary) #Holds the intermediate dataframe for each thicket

    for curr_thicket_indx, thicket in enumerate(thicket_dictionary.items()):
        cols = [str(c) for c in columns[curr_thicket_indx]]
        sub_dataframes[curr_thicket_indx] = thicket[1].dataframe[[("name", ""), *columns[curr_thicket_indx]]].reset_index()
        sub_dataframes[curr_thicket_indx].columns = sub_dataframes[idx].columns.to_flat_index().map(column_name_mapper)
        sub_dataframes[curr_thicket_indx]["name"] = thicket[1].dataframe["name"].tolist()
        
        melted_df = pd.melt(
            sub_dataframes[curr_thicket_indx],
            id_vars=["node", "name"],
            value_vars=cols,
            var_name="Performance counter",
            value_name=" ",
        )

        position = []

        #Grab the indices of the nodes that we are interested in
        for node in nodes[curr_thicket_indx]:
            idx_c = melted_df.index[melted_df["node"] == node]
            for pos in idx_c:
                position.append(pos)

        # rename columns such that the x-axis label is "node" and not "name", tick marks
        # will be node names
        #This is where we grab the actual data for a node that was passed in!
        filtered_dfs[idx] = melted_df.loc[position].rename( columns={"node": "hatchet node", "name": "node"})
        thicket_name = [str(thicket[0])] * len(filtered_dfs[idx]["node"])
        filtered_dfs[idx]["node"] = thicket_name
        display(filtered_dfs[idx])

    master_df = pd.concat(filtered_dfs, ignore_index=True)
    if set_y_axis_log == True:
        master_df[" "] = np.log(master_df[" "])
    # 
    display(master_df)

    return sns.violinplot( data=master_df, x="node", y=" ", hue="Performance counter", **kwargs)