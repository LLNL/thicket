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

def _column_name_mapper(current_cols):
    """
        Internal function that returns string representation of current_cols

        Parameters:
            current_cols: 
    """
    if current_cols[0] in ["node", "name"]:
        return current_cols[0]

    return str(current_cols)

def _add_percentile_lines_node(graph, thicket, nodes, columns, percentiles_vals, lines_styles=None, line_colors = None):

    violin_idx = -1

    #Default line styles and line colors
    if lines_styles == None or len(lines_styles) < len(percentiles_vals):
        lines_styles += ["-"] * (len(percentiles_vals - len(percentiles_vals)))
    if line_colors == None or len(line_colors) < len(percentiles_vals):
        line_colors += ["black"] * (len(percentiles_vals) - len(percentiles_vals))

    for node in nodes:
        for column in columns:
            violin_idx += 1
            for idx, percentile in enumerate(percentiles_vals):
                stats_column = None
                #Columnar joined thickets
                if type(column) == type(tuple()):
                    stats_column = (str(column[0]), "{}_percentiles_{}".format(column[1], int(percentile * 100)))
                #Non-columnar joined
                else:
                    stats_column = "{}_percentiles_{}".format(column, int(percentile * 100))
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



def display_violinplot(thicket, nodes=[], columns=[], percentiles = [], percentile_linestyles = [], percentile_colors = [], **kwargs):
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

    filtered_df = None

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
    # columnar joined thicket object
    else:
        cols = [str(c) for c in columns]
        df_subset = thicket.dataframe[[("name", ""), *columns]].reset_index()
        df_subset.columns = df_subset.columns.to_flat_index().map(_column_name_mapper)
        df_subset["name"] = thicket.dataframe["name"].tolist()
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
        # print(filtered_df)
        # print(filtered_df.columns)
        if len(columns) > 1:
            #return px.violin(filtered_df)
            #return px.violin(filtered_df, x="node", y=' ', box=True, points="all", hover_data=['Category'], title='Violin Plot')
            return sns.violinplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else:
            return sns.violinplot(data=filtered_df, x="node", y=" ", **kwargs)
