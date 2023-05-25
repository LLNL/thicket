# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
from ..utils import verify_thicket_structures


def variance(thicket, columns=None):
    """Calculate standard deviation and variance per node.

    Designed to take in a Thicket, and will append a column to the statsframe
    for the standard deviation and variance calculations per node.

    Variance will allow you to see the spread within a dataset and standard
    deviation will tell you how dispersed the data is in relation to the mean.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform deviation calculations on
    """
    if columns is None:
        raise ValueError("To see a list of valid columns run get_perf_columns().")

    #verify_thicket_structures(
    #    thicket.dataframe, index=["node", "profile"], columns=columns
    #)

    if thicket.dataframe.columns.nlevels == 1:
        for column in columns:
            var = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                var.append(np.var(thicket.dataframe.loc[node][column]))
            thicket.statsframe.dataframe[column + "_var"] = var

    else:
        for idx,column in columns:
            var = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                var.append(np.var(thicket.dataframe.loc[node][(idx,column)]))
            thicket.statsframe.dataframe[(idx,column + "_var")] = var

        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
