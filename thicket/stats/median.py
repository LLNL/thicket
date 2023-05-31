# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd

from ..utils import verify_thicket_structures


def median(thicket, columns=None):
    """Calculate the median for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the
    aggregated statistics table for the median calculation for each node.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to perform median calculation
            on. Note, if using a columnar joined thicket a list of tuples must be passed
            in with the format (column index, column name).
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
            median = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                median.append(np.median(thicket.dataframe.loc[node][column]))
            thicket.statsframe.dataframe[column + "_median"] = median
    # columnar joined thicket object
    else:
        for idx, column in columns:
            median = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                median.append(np.median(thicket.dataframe.loc[node][(idx, column)]))
            thicket.statsframe.dataframe[(idx, column + "_median")] = median

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
