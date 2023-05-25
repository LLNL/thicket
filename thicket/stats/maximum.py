# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
from ..utils import verify_thicket_structures


def maximum(thicket, columns=None):
    """Calculate the maximum value per node.

    Designed to take in a single indexed or a columnar join thicket, and will append a column to the statsframe
    for the maximum value for all profiles per node.

    The maxiumum is the highest observation in the dataset.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform extremnum calculations on
    """
    if columns is None:
        raise ValueError("To see a list of valid columns run get_perf_columns().")

    #verify_thicket_structures(
    #    thicket.dataframe, index=["node", "profile"], columns=columns
    #)

    if thicket.dataframe.columns.nlevels == 1:
        for column in columns:
            maximum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                maximum.append(max(thicket.dataframe.loc[node][column]))
            thicket.statsframe.dataframe[column + "_max"] = maximum
    else:
        for idx,column in columns:
            maximum = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                maximum.append(max(thicket.dataframe.loc[node][(idx,column)]))
            thicket.statsframe.dataframe[(idx,column + "_max")] = maximum

        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
