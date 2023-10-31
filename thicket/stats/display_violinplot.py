# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
import hatchet as ht
import numpy as np
import matplotlib as mpl

from .percentiles import percentiles
from ..utils import verify_thicket_structures

def _add_percentile_lines_node(graph, thicket, nodes, columns, percentiles_vals, lines_styles=None, line_colors = None):

    violin_idx = -1

    #Default line styles and line colors
    if lines_styles == None or len(lines_styles) == 0:
        lines_styles = ["-"] * len(percentiles_vals)
    if line_colors == None or len(line_colors) == 0:
        line_colors = ["black"] * len(percentiles_vals)

    for node in nodes:
        for column in columns:
            violin_idx += 1
            for idx, percentile in enumerate(percentiles_vals):
                stats_column = None
                if type(column) == type(tuple()):
                    stats_column = (str(column[0]), "{}_percentiles_{}".format(column[1], int(percentile * 100)))
                #Call percentile(...) if the percentile value for the column has not been calculated already
                if column in thicket.exc_metrics and stats_column not in thicket.statsframe.exc_metrics\
                    or\
                    column in thicket.inc_metrics and stats_column not in thicket.statsframe.inc_metrics:
                        percentiles(thicket, [column], [percentile])
                percentile_value = thicket.statsframe.dataframe[stats_column][node]

                #Plot line
                added_line = graph.axhline(y=percentile_value, color=line_colors[idx], linestyle = lines_styles[idx])
                #Get the violin structure
                patch =  mpl.patches.PathPatch(graph.collections[violin_idx].get_paths()[0], transform = graph.transData)
                #Clip line to the violin structure
                added_line.set_clip_path(patch)

    return graph

def display_violinplot(thicket, nodes=[], columns=[], percentile_line_values = [], percentile_line_styles = [], percentile_line_colors = [], **kwargs):
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

        #User specified percentile value lines to plot
        if len(percentile_line_values) != 0:
            if len(columns) > 1:
                return _add_percentile_lines_node(
                        sns.violinplot( data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs), 
                        thicket, 
                        nodes, 
                        columns, 
                        percentile_line_values,
                        percentile_line_styles,
                        percentile_line_colors), filtered_df
            else: 
                return _add_percentile_lines_node(
                        sns.violinplot(data=filtered_df, x="node", y=" ", **kwargs),
                        thicket,
                        nodes,
                        columns,
                        percentile_line_values,
                        percentile_line_styles,
                        percentile_line_colors)
        else: #User did not specify percentiles, just return violinplot
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
        sub_dataframes[curr_thicket_indx].columns = sub_dataframes[curr_thicket_indx].columns.to_flat_index().map(column_name_mapper)
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
        # This is where we grab the actual data for a node that was passed in!
        filtered_dfs[curr_thicket_indx] = melted_df.loc[position].rename( columns={"node": "hatchet node", "name": "node"})
        thicket_name = [str(thicket[0]) + str()] * len(filtered_dfs[curr_thicket_indx]["node"])
        filtered_dfs[curr_thicket_indx]["node"] = thicket_name
        #display(filtered_dfs[curr_thicket_indx])

    master_df = pd.concat(filtered_dfs, ignore_index=True)
   
    #display(master_df)

    return sns.violinplot( data=master_df, x="node", y=" ", hue="Performance counter", **kwargs)