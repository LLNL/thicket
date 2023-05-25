# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns
from ..utils import verify_thicket_structures


def display_histogram(thicket, node=None, column=None, **kwargs):
    """Display a histogram.

    Arguments:
        thicket (thicket): Thicket object
        node (str): node object
        column (str): column from ensemble frame

    Returns:
        (matplotlib Axes): object for managing plot
    """
    if column is None or node is None:
        raise ValueError("To see a list of valid columns run get_perf_columns().")

    #verify_thicket_structures(
    #    thicket.dataframe, index=["node", "profile"], columns=[column]
    #)

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

    else:
        col_idx,column_value = column[0],column[1]
        df_subset = thicket.dataframe[col_idx]

        df = pd.melt(
            df_subset.reset_index(),
            id_vars="node",
            value_vars=column_value,
            value_name= " ",
        )

        filtered_df = df[df["node"] == node]

        ax = sns.displot(filtered_df,x=" ", kind="hist", **kwargs)

        return ax
