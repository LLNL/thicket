# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd

from ..utils import verify_thicket_structures


def maximum(thicket, columns=None):
    """Determine the maximum for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the maximum value for each node.

    The maximum is the highest observation for a node and its associated profiles.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to determine maximum value for.
            Note, if using a columnar joined thicket a list of tuples must be passed in
            with the format (column index, column name).
    """
    if columns is None:
        raise ValueError(
            "To see a list of valid columns, please run Thicket.get_perf_columns()."
        )

    verify_thicket_structures(
        thicket.dataframe, index=["node", "profile"], columns=columns
    )

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        for column in columns:
            maximum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                maximum.append(max(thicket.dataframe.loc[node][column]))
            thicket.statsframe.dataframe[column + "_max"] = maximum
    # columnar joined thicket object
    else:
        for idx, column in columns:
            maximum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                maximum.append(max(thicket.dataframe.loc[node][(idx, column)]))
            thicket.statsframe.dataframe[(idx, column + "_max")] = maximum

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
