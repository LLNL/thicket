# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd


def calc_deviation(thicket=None, columns=None):
    """Calculate standard deviation and variance per node.

    Designed to take in a Thicket, and will append a column to the statsframe
    for the standard deviation and variance calculations per node.

    Variance will allow you to see the spread within a dataset and standard
    deviation will tell you how dispersed the data is in relation to the mean.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform deviation calculations on
    """
    for column in columns:
        var = []
        std = []
        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            var_value = np.var(thicket.dataframe.loc[node][column])
            std_value = np.std(thicket.dataframe.loc[node][column])
            var.append(var_value)
            std.append(std_value)
        thicket.statsframe.dataframe[column + "_var"] = var
        thicket.statsframe.dataframe[column + "_std"] = std
