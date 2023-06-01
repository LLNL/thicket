# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd

from ..utils import verify_thicket_structures


def minimum(thicket, columns=None):
    """Determine the minimum for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the minimum value for each node.

    The minimum is the lowest observation for a node and its associated profiles.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to determine minimum value for.
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
            minimum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                minimum.append(min(thicket.dataframe.loc[node][column]))
            thicket.statsframe.dataframe[column + "_min"] = minimum
    # columnar joined thicket object
    else:
        for idx, column in columns:
            minimum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                minimum.append(min(thicket.dataframe.loc[node][(idx, column)]))
            thicket.statsframe.dataframe[(idx, column + "_min")] = minimum

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
