# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd


def calc_average(thicket=None, columns=None):
    """Calculate median and mean per node.

    Designed to take in a Thicket, and will append a column to the statsframe for
    the median and mean calculations per node.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform average calculations on
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
