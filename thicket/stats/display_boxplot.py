# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
import hatchet as ht

from ..utils import verify_thicket_structures


def display_boxplot(thicket, nodes=[], columns=[], **kwargs):
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
    for node in nodes:
        if not isinstance(node, ht.node.Node):
            raise ValueError(
                "Value(s) passed to node argument must be of type hatchet.node.Node."
            )

    verify_thicket_structures(
        thicket.dataframe, index=["node", "profile"], columns=columns
    )

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
            return sns.boxplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else:
            return sns.boxplot(data=filtered_df, x="node", y=" ", **kwargs)
    # columnar joined thicket object
    else:
        initial_idx, initial_col = columns[0][0], columns[0][1]
        cols = [initial_col]
        for i in range(1, len(columns)):
            if initial_idx != columns[i][0]:
                raise ValueError(
                    "Columns specified as tuples must have the same column index (first element)"
                )
            else:
                cols.append(columns[i][1])

        df_subset = thicket.dataframe[initial_idx].reset_index()
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

        if len(columns) > 1:
            return sns.boxplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else:
            return sns.boxplot(data=filtered_df, x="node", y=" ", **kwargs)
