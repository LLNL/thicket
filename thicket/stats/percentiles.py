# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd

from ..utils import verify_thicket_structures


def percentiles(thicket, columns=None):
    """Calculate the q-th percentile for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the q-th percentile calculation for each node.

    The 25th percentile is the lower quartile, and is the value at which 25% of the
    answers lie below that value.

    The 50th percentile, is the median and half of the values lie below the median and
    half lie above the median.

    The 75th percentile is the upper quartile, and is the value at which 25% of the
    answers lie above that value and 75% of the answers lie below that value.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to perform percentile
            calculation on. Note if using a columnar joined thicket a list of tuples
            must be passed in with the format (column index, column name).
    """
    if columns is None:
        raise ValueError(
            "To see a list of valid columns, run 'Thicket.performance_cols'."
        )

    verify_thicket_structures(thicket.dataframe, index=["node"], columns=columns)

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        # select numeric columns within thicket (.quantiles) will not work without this step
        numerics = ["int16", "int32", "int64", "float16", "float32", "float64"]
        df_num = thicket.dataframe.select_dtypes(include=numerics)
        df = df_num.reset_index().groupby("node").quantile([0.25, 0.50, 0.75])
        for column in columns:
            percentiles = []
            for node in pd.unique(df.reset_index()["node"].tolist()):
                percentiles.append(list(df.loc[node][column]))
            thicket.statsframe.dataframe[column + "_percentiles"] = percentiles
            # check to see if exclusive metric
            if column in thicket.exc_metrics:
                thicket.statsframe.exc_metrics.append(column + "_percentiles")
            # check to see if inclusive metric
            else:
                thicket.statsframe.inc_metrics.append(column + "_percentiles")
    # columnar joined thicket object
    else:
        numerics = ["int16", "int32", "int64", "float16", "float32", "float64"]
        df_num = thicket.dataframe.select_dtypes(include=numerics)
        df = df_num.reset_index(level=1).groupby("node").quantile([0.25, 0.50, 0.75])
        percentiles = []
        for idx, column in columns:
            percentiles = []
            for node in pd.unique(df.reset_index()["node"].tolist()):
                percentiles.append(list(df.loc[node][(idx, column)]))
            thicket.statsframe.dataframe[(idx, column + "_percentiles")] = percentiles
            # check to see if exclusive metric
            if (idx, column) in thicket.exc_metrics:
                thicket.statsframe.exc_metrics.append((idx, column + "_percentiles"))
            # check to see if inclusive metric
            else:
                thicket.statsframe.inc_metrics.append((idx, column + "_percentiles"))

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
