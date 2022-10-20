# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd


def calc_extremum(thicket=None, columns=None):
    """Calculate min and max per node.

    Designed to take in a Thicket, and will append a column to the statsframe
    for the min and max calculations per node.

    The minimum is the lowest observation and the maxiumum is the highest
    observation in the dataset.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform extremnum calculations on
    """
    for column in columns:
        minimum = []
        maximum = []
        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            min_value = min(thicket.dataframe.loc[node][column])
            max_value = max(thicket.dataframe.loc[node][column])
            minimum.append(min_value)
            maximum.append(max_value)
        thicket.statsframe.dataframe[column + "_min"] = minimum
        thicket.statsframe.dataframe[column + "_max"] = maximum
