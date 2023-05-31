# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
import hatchet as ht

from ..utils import verify_thicket_structures


def display_histogram(thicket, node=None, column=None, **kwargs):
    """Display a histogram for a user passed node and column. Node and column must come
    from the performance data table.

    Arguments:
        thicket (thicket): Thicket object
        node (node): Node object
        column (str): Column from performance data table. Note, if using a
            columnar joined thicket a tuple must be passed in with the format
            (column index, column name).

    Returns:
        (matplotlib Axes): Object for managing histogram plot.
    """
    if column is None or node is None:
        raise ValueError(
            "To see a list of valid columns, run Thicket.get_perf_columns()."
        )

    if not isinstance(node, ht.node.Node):
        raise ValueError(
            "Value passed to node argument must be of type hatchet.node.Node."
        )

    verify_thicket_structures(
        thicket.dataframe, index=["node", "profile"], columns=[column]
    )

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        df = pd.melt(
            thicket.dataframe.reset_index(),
            id_vars="node",
            value_vars=column,
            value_name=" ",
        )

        filtered_df = df[df["node"] == node]

        ax = sns.displot(filtered_df, x=" ", kind="hist", **kwargs)

        return ax
    # columnar joined thicket object
    else:
        col_idx, column_value = column[0], column[1]
        df_subset = thicket.dataframe[col_idx]

        df = pd.melt(
            df_subset.reset_index(),
            id_vars="node",
            value_vars=column_value,
            value_name=" ",
        )

        filtered_df = df[df["node"] == node]

        ax = sns.displot(filtered_df, x=" ", kind="hist", **kwargs)

        return ax
