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
