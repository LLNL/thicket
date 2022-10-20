# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import numpy as np


def calc_percentile(thicket=None, columns=None):
    """Calculate q-th percentile per node.

    Designed to take in a Thicket, and will append a column to the statsframe
    for the q-th percentile of the data per node.

    The 25th percentile is the lower quartile, and is the value at which 25% of
    the answers lie below that value.

    The 50th percentile, is the median and half of the values lie below the
    median and half lie above the median.

    The 75th percentile is the upper quartile, and is the value at which 25% of
    the answers lie above that value and 75% of the answers lie below that
    value.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics to perform percentile calculations on
    """
    for column in columns:
        percentiles = []
        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            perc_value = np.percentile(
                thicket.dataframe.loc[node][column], [25, 50, 75]
            )
            percentiles.append(perc_value)
        thicket.statsframe.dataframe[column + "_percentile"] = percentiles
