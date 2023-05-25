# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns


def display_boxplot(thicket, nodes=[], columns=[], **kwargs):
    """Designed to take in a Thicket, and plot a boxplot based on user passed node's and a single metric.

    Arguments:
        thicket (thicket) : Thicket object
        nodes (list) : List of nodes to view on the x-axis
        column (str) : A single hardware or timing metric to view on the y-axis

    Returns:
        plot: Returns a boxplot
    """



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

        filtered_df = df.loc[position].rename(
            columns={"node": "hatchet node", "name": "node"}
        )

        if len(columns) > 1:
            return sns.boxplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else:
            return sns.boxplot(data=filtered_df, x="node", y=" ", **kwargs)

    else:
        initial_idx,initial_col = columns[0][0],columns[0][1]
        cols = [initial_col]
        for element in columns[1:len(columns)]:
            if initial_idx != element[0]:
                raise ValueError("The columns argument must have the same column index throughout for a columnar joined thicket.")
            else: 
                cols.append(element[1])

        df_subset = thicket.dataframe[initial_idx].reset_index()
        df_subset["name"] = thicket.dataframe["name"].tolist()

        df = pd.melt(
             df_subset,
             id_vars=["node","name"],
             value_vars=cols,
             var_name="Performance counter",
             value_name=" ",
        )

        position = []
        for node in nodes:
            idx = df.index[df["node"] == node]
            for pos in idx:
                position.append(pos)

        filtered_df = df.loc[position].rename(
            columns = {"node": "hatchet node", "name": "node"}
        )

        if len(columns) > 1:
            return sns.boxplot(
                data=filtered_df, x="node", y=" ", hue="Performance counter", **kwargs
            )
        else: 
            return sns.boxplot(data=filtered_df, x="node", y=" ", **kwargs)
