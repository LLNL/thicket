# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd


def calc_average(thicket=None, columns=None):
    """
    Designed to take in a Thicket, and will append a column to the statsframe for the
    median and mean calculations per node.

    Arguments/Parameters
    _ _ _ _ _ _ _ _ _ _ _

    thicket : A thicket

    columns : List of hardware/timing metrics to perform extremum calculations on

    Returns
    _ _ _ _ _ _ _ _ _ _ _

    thicket: Returns a thicket

    """

    for column in columns:
        median = []
        mean = []
        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            median_value = np.median(thicket.dataframe.loc[node][column])
            mean_value = np.mean(thicket.dataframe.loc[node][column])
            median.append(median_value)
            mean.append(mean_value)
        thicket.statsframe.dataframe[column + "_median"] = median
        thicket.statsframe.dataframe[column + "_mean"] = mean

    return thicket
