# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
import hatchet as ht
import plotly.express as px

from ..utils import verify_thicket_structures

def display_violinplot(thicket, nodes=[], columns=[], **kwargs):
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

    print(kwargs)
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

    #Pave meeting: Discuss what melting function is doing, 

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